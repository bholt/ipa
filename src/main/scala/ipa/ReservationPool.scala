package ipa

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Function

import com.datastax.driver.core.{ConsistencyLevel => CLevel}
import com.twitter.util
import com.twitter.util.{Future => TwFuture}
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.column.{MapColumn, PrimitiveColumn}
import com.websudos.phantom.dsl._
import org.joda.time.DateTime
import owl.Connector.config
import owl.{Timestamped, Tolerance}
import owl.Util._
import ipa.{thrift => th}

import scala.concurrent._

case class Alloc(key: UUID, allocs: Map[Int, Long] = Map(), leases: Map[Int,DateTime] = Map()) {
  private def interpret(a: Long, currentMax: Long) = if (a == Alloc.Max) currentMax else a
  def total(currentMax: Long) =
    allocs.values.map(interpret(_, currentMax)).sum
  def allocated(currentMax: Long) = {
    interpret(allocs.getOrElse(this_host_hash, 0L), currentMax)
  }
}

object Alloc {
  val Max = -1L
}

object ReservationPool {

}

class ReservationPool(baseName: String, tolerance: Tolerance)(implicit imps: CommonImplicits) {
  import imps._

  object m {
    val rpcs = metrics.create.counter("rpcs")
    val outOfBounds = metrics.create.counter("out_of_bounds")
    val refreshes = metrics.create.counter("refreshes")
    val immediates = metrics.create.counter("immediates")
    val reads = metrics.create.counter("reads")
    val incrs = metrics.create.counter("incrs")
    val errors = metrics.create.counter("errors")
    val allocs = metrics.create.counter("allocs")
    val unallocs = metrics.create.counter("unallocs")
    val reallocs = metrics.create.counter("reallocs")
    val initialized = metrics.create.counter("initialized")

    val cached_reads = metrics.create.counter("cached_reads")

    val races = metrics.create.counter("races")

    val latencyWeakWrite   = metrics.create.timer("weak_write_latency")
    val latencyStrongWrite = metrics.create.timer("strong_write_latency")
    val latencyWeakRead    = metrics.create.timer("weak_read_latency")
    val latencyStrongRead  = metrics.create.timer("strong_read_latency")

    val latencyAllocUpdate = metrics.create.timer("alloc_update_latency")
    val latencyAllocRead   = metrics.create.timer("alloc_read_latency")
  }

  object alloc {
    val latencyAllocUpdate = metrics.create.timer("alloc_update_latency")

    class AllocTable extends CassandraTable[AllocTable, Alloc] {
      object key extends UUIDColumn(this) with PartitionKey[UUID]
      object map extends MapColumn[AllocTable, Alloc, Int, Long](this)
      object leases extends MapColumn[AllocTable, Alloc, Int, DateTime](this)
      override val tableName = baseName + "_allocs"
      override def fromRow(r: Row) = Alloc(key(r), map(r), leases(r))
    }

    val table = new AllocTable

    object prepared {
      private val (k, a, l) = (table.key.name, table.map.name, table.leases.name)
      private val tname = s"${space.name}.${table.tableName}"

      val get: (UUID) => (CLevel) => BoundOp[Alloc] = {
        val ps = session.prepare(s"SELECT * FROM $tname WHERE $k = ? LIMIT 1")
        key: UUID => ps.bindWith(key) {
          _.first.map(table.fromRow).getOrElse(Alloc(key))
        }
      }

      val update: (UUID, Long) => (CLevel) => BoundOp[Unit] = {
        val ps = session.prepare(s"UPDATE $tname SET $a=$a+?, $l=$l+? WHERE $k=?")
        (key: UUID, alloc: Long) => {
          val me = this_host_hash
          val lease = DateTime.now().plus(config.reservations.lease_period)
          ps.bindWith(Map(this_host_hash -> alloc), Map(me -> lease), key)(_ => ())
        }
      }
    }

    def get(key: UUID): TwFuture[Alloc] =
      prepared.get(key)(CLevel.ONE).execAsTwitter()

    def update(key: UUID, alloc: Long): TwFuture[Unit] =
      prepared.update(key, alloc)(CLevel.ALL).execAsTwitter()
          .instrument(latencyAllocUpdate)

  }

  def init(): ReservationPool = {
    Console.err.println("# Create allocations table")
    if (config.do_reset) blocking {
      session.execute(s"DROP TABLE IF EXISTS ${space.name}.${alloc.table.tableName}")
    }
    alloc.table.create.ifNotExists().future().await()

    // TODO: try reservations with UDFs
    // session.execute("CREATE OR REPLACE FUNCTION reservations.alloc_total (alloc map<int,bigint>) RETURNS NULL ON NULL INPUT RETURNS b  igint LANGUAGE java AS 'long total = 0; for (Object e : alloc.values()) total += (Long)e; return total;';")

    this
  }

  val reservationMap = new ConcurrentHashMap[UUID, Reservation]

  def get(key: UUID): Reservation = {
    reservationMap.computeIfAbsent(key, new Function[UUID, Reservation] {
      override def apply(key: UUID): Reservation = new Reservation(tolerance)
    })
  }

  def clear(): Unit = {
    reservationMap.clear()
  }

  class Reservation(tol: Tolerance) {
    var lastRead: Timestamped[Long] = Timestamped(0L, 0L)
    var total: Long = 0L      // tokens allocated globally (currently assumed to be the max possible given the error tolerance)
    var allocated: Long = 0L  // tokens allocated to this replica locally
    var available: Long = 0L // local tokens remaining

    var lease: Option[DateTime] = None
    var updating = false

    def leaseExpiresSoon = lease match {
      case Some(l) => l.minus(config.reservations.soon_period).isBeforeNow
      case None => false
    }

    def max = tol.delta(lastRead.value)
    def maxLocal = max / session.nreplicas

    def update(read: Timestamped[Long], allocOpt: Option[Alloc] = None): Unit = {
      lastRead = read
      allocOpt match {
        case Some(alloc) =>
          total = alloc.total(maxLocal)
          allocated = alloc.allocated(maxLocal)
          available = allocated
          lease = alloc.leases.get(this_host_hash)
        case None =>
          // just get back our allocated tokens
          available = allocated
      }
    }

    def refresh(table: IPACounter, key: UUID) = {
      m.refreshes += 1
      fetchAndUpdate(table, key, CLevel.ALL)
    }

    def fetchAndUpdate(table: IPACounter, key: UUID, cons: CLevel): TwFuture[Reservation] = {
      val rt = System.nanoTime
      val f_read =
        table.readTwitter(cons)(key)
            .instrument(m.latencyWeakRead).instrument()
      val f_alloc =
        alloc.get(key).instrument(m.latencyAllocRead)

      TwFuture.join(f_read, f_alloc) map {
        case (v, allocs) =>
          val vt = Timestamped(v, rt)
          this.update(vt, Some(allocs))

          // if our lease has expired, fire off an update to set it to 0
          for (l <- lease if l.isBeforeNow && allocated > 0 && !updating) {
            updating = true
            allocate(key, 0) onSuccess { _ =>
              updating = false
            }
          }

          this
      }
    }

    def allocate(key: UUID, n: Long): TwFuture[Unit] = {
      if (n >= allocated) m.allocs += 1 else m.unallocs += 1
      alloc.update(key, n)
    }

    def consume(n: Long, exec: CLevel => TwFuture[Unit]): TwFuture[Unit] = {
      if (available >= n) { // enough tokens available:
        available -= n
        exec(CLevel.ONE)
      } else { // race where some other op took the token
        m.races += 1
        // rather than possibly iterating again, just give up and wait
        exec(CLevel.ALL)
      }
    }

    def used = allocated - available
    def delta = total - used

    def interval = th.IntervalLong(lastRead.value - delta, lastRead.value + delta)

    override def toString = s"Reservation(read: $lastRead, total: $total, alloc: $allocated, avail: $available)"
  }
}
