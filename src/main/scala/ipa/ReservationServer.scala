package ipa

import java.net.InetAddress
import java.util.UUID

import scala.concurrent.duration.{Deadline, Duration, NANOSECONDS}
import scala.util.{Failure, Success, Try}
import com.datastax.driver.core.{BoundStatement, Cluster, ConsistencyLevel => CLevel}
import com.twitter.finagle.loadbalancer.Balancers
import com.twitter.finagle.Thrift
import com.twitter.util._
import com.twitter.{util => tw}
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl.{UUID, _}
import ipa.IPACounter.WeakOps
import ipa.thrift._
import ipa.{thrift => th}
import org.joda.time.{DateTime, Period}
import owl.Connector.config
import owl.Util._
import owl.{Connector, Consistency, OwlService, Tolerance}

import scala.concurrent.blocking
import scala.collection.concurrent
import scala.collection.JavaConversions._
import scala.collection.mutable
import owl.Consistency._

import scala.collection.immutable.HashMap

case class Timestamped[T](value: T, time: Long = System.nanoTime) {
  def expired: Boolean = (System.nanoTime - time) > config.lease.periodNanos
  def get: Option[T] = if (!expired) Some(value) else None
}

case class Alloc(key: UUID, allocs: Map[Int, Long] = Map(), leases: Map[Int,DateTime] = Map()) {
  def total = allocs.values.sum
  def allocated = allocs.getOrElse(this_host_hash, 0L)
}

class ReservationServer(implicit imps: CommonImplicits) extends th.ReservationService[tw.Future] {
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

    class AllocTable extends CassandraTable[AllocTable, Alloc] {
      object key extends UUIDColumn(this) with PartitionKey[UUID]
      object map extends MapColumn[AllocTable, Alloc, Int, Long](this)
      object leases extends MapColumn[AllocTable, Alloc, Int, DateTime](this)
      override val tableName = "allocs"
      override def fromRow(r: Row) = Alloc(key(r), map(r), leases(r))
    }

    val table = new AllocTable

    object prepared {
      private val (k, a, l) = (table.key.name, table.map.name, table.leases.name)
      private val tname = s"${space.name}.${table.tableName}"

      val get: (UUID) => (CLevel) => BoundStatement = {
        val ps = session.prepare(s"SELECT * FROM $tname WHERE $k = ? LIMIT 1")
        key: UUID => ps.bindWith(key)
      }

      val update: (UUID, Long) => CLevel => BoundStatement = {
        val ps = session.prepare(s"UPDATE $tname SET $a=$a+?, $l=$l+? WHERE $k=?")
        (key: UUID, alloc: Long) => {
          val me = this_host_hash
          val lease = DateTime.now().plus(config.reservations.lease_period)
          ps.bindWith(Map(this_host_hash -> alloc), Map(me -> lease), key)
        }
      }
    }

    def get(key: UUID): Future[Alloc] =
      prepared.get(key)(CLevel.ONE).execAsTwitter()
          .first(table.fromRow) map { _.getOrElse(Alloc(key)) }

    def update(key: UUID, alloc: Long) =
      prepared.update(key, alloc)(CLevel.ALL).execAsTwitter()
          .instrument(m.latencyAllocUpdate).unit

  }

  def init(): ReservationServer = {
    Console.err.println("Creating allocations table")
    if (config.do_reset) blocking {
      session.execute(s"DROP TABLE IF EXISTS ${space.name}.${alloc.table.tableName}")
    }
    alloc.table.create.ifNotExists().future().await()

    // TODO: try reservations with UDFs
    // session.execute("CREATE OR REPLACE FUNCTION reservations.alloc_total (alloc map<int,bigint>) RETURNS NULL ON NULL INPUT RETURNS b  igint LANGUAGE java AS 'long total = 0; for (Object e : alloc.values()) total += (Long)e; return total;';")

    this
  }

  case class Entry(
      table: IPACounter,
      space: KeySpace,
      tolerance: Tolerance
  ) {
    val reservations = new mutable.HashMap[UUID, Reservation]

    def reservation(key: UUID): Future[Reservation] = {
      val r = reservations.getOrElseUpdate(key, new Reservation)
      if (r.lastRead.time == 0L) {
        m.initialized += 1
        r.fetchAndUpdate(table, key, Weak)
      } else {
        tw.Future.value(r)
      }
    }

  }

  class Reservation {
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

    def max(tol: Tolerance) = tol.delta(lastRead.value)

    def update(read: Timestamped[Long], allocOpt: Option[Alloc] = None): Unit = {
      lastRead = read
      allocOpt match {
        case Some(alloc) =>
          total = alloc.total
          allocated = alloc.allocated
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

    def fetchAndUpdate(table: IPACounter, key: UUID, cons: CLevel): Future[Reservation] = {
      val rt = System.nanoTime
      val f_read =
        table.readTwitter(cons)(key)
          .instrument(m.latencyWeakRead).instrument()
      val f_alloc =
        alloc.get(key).instrument(m.latencyAllocRead)

      tw.Future.join(f_read, f_alloc) map {
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

    def allocate(key: UUID, n: Long): Future[Unit] = {
      if (n >= allocated) m.allocs += 1 else m.unallocs += 1
      alloc.update(key, n)
    }

    def consume(n: Long, exec: CLevel => tw.Future[Unit]): Future[Unit] = {
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

    def interval = th.IntervalLong(lastRead.value, lastRead.value + delta)

    override def toString = s"Reservation(read: $lastRead, total: $total, alloc: $allocated, avail: $available)"
  }

  object Reservation {
    def apply(entry: Entry, lastRead: Timestamped[Long], allocOpt: Option[Alloc]): Reservation = {
      val r = new Reservation()
      r.update(lastRead, allocOpt)
      r
    }
  }

  val tables = new concurrent.TrieMap[Table, Entry]

  def table(t: Table): Try[Entry] = {
    Try {
      tables.getOrElseUpdate(t, {
        Console.err.println(s"creating: $t")
        implicit val space = KeySpace(t.space)
        implicit val imps = CommonImplicits()
        IPACounter.fromName(t.name) map {
          case tbl: IPACounter with IPACounter.ErrorTolerance =>
            Entry(tbl, space, tbl.tolerance)
          case tbl =>
            Entry(tbl, space, Tolerance(0))
        } recoverWith {
          case e: Throwable =>
            sys.error(s"Unable to create table ($t): ${e.getMessage}")
        } get
      })
    } recoverWith { case e =>
      Failure(ReservationException(s"counter without error tolerance: $t"))
    }
  }

  /**
    * Initialize new UuidSet
    * TODO: make generic version
    */
  override def createUuidset(t: Table, sizeTolerance: Double): tw.Future[Unit] = ???


  /** Initialize new Counter table. */
  override def createCounter(t: Table, error: Double): tw.Future[Unit] = {
    implicit val space = KeySpace(t.space)
    implicit val imps = CommonImplicits()
    val counter = new IPACounter(t.name) with WeakOps
    tables += (t -> Entry(counter, space, Tolerance(error)))
    m.rpcs += 1
    tw.Future.Unit
  }

  override def readInterval(t: Table, keyStr: String): tw.Future[th.IntervalLong] = {
    m.rpcs += 1
    m.reads += 1
    val key = keyStr.toUUID
    table(t) match {
      case Success(e) =>
        // try to find cached read in reservation
        e.reservation(key) flatMap { res => // if we found a reservation
          if (res.lastRead.expired) {
            res.fetchAndUpdate(e.table, key, Weak) map { res =>
              res.interval
            }
          } else {
            m.cached_reads += 1
            // return the cached value
            tw.Future.value(res.interval)
          }
        }
      case Failure(e) =>
        tw.Future.exception(e)
    }
  }

  override def incr(t: Table, keyStr: String, n: Long): tw.Future[Unit] = {
    m.rpcs += 1
    m.incrs += 1
    val key = keyStr.toUUID

    table(t) match {
      case Success(e) =>

        // execute the increment on durable store
        val exec = { cons: CLevel =>
          val timer = if (cons == Strong) m.latencyStrongWrite
                      else m.latencyWeakWrite
          e.table.incrTwitter(cons)(key, n).instrument(timer).instrument()
        }

        // may need to get the latest value of the counter if we haven't created a reservation for this record yet
        e.reservation(key) flatMap { res =>
          // being conservative and not allowing one replica to hoard
          // TODO: try allowing them to exceed this, since it's likely that we'll have cases where the nearest replica is being hit more
          val max = res.max(e.tolerance) / session.nreplicas

          if (n > max) {
            m.outOfBounds += 1
            exec(CLevel.ALL) map { _ =>
              blocking(Thread.sleep(config.lease.periodMillis))
            }
          } else {
            for {
              preallocated <-
                if (n <= res.allocated && !res.leaseExpiresSoon) {
                  tw.Future.value(true)
                } else {
                  // we need to allocate more, or lease is about to expire
                  if (res.leaseExpiresSoon) m.reallocs += 1
                  res.allocate(key, max).map(_ => false)
                }

              immediate <-
                if (n <= res.available) tw.Future.value(true)
                else res.refresh(e.table, key).map(_ => false)

              _ <-
                if (res.available >= n) {
                  res.available -= n
                  if (preallocated && immediate) m.immediates += 1
                  exec(CLevel.ONE)
                } else {
                  m.races += 1
                  exec(CLevel.ALL)
                }
            } yield ()
          }
        }
      case Failure(e) =>
        tw.Future.exception(e)
    }
  }

  override def setOp(tbl: Table, op: SetOp): Future[Result] = ???

//  override def create(tbl: Table, datatype: Datatype, tolerance: Double): Future[Unit] = ???

  override def metricsJson(): tw.Future[String] = {
    // session.getCluster.getConfiguration.getPolicies.getLoadBalancingPolicy
    val latencyStats = Connector.latencyMonitor.getScoresSnapshot.getAllStats
    println(
      latencyStats map {
        case (host,stats) =>
          s"${host.getAddress}:\n - ${stats.getLatencyScore/1e6} ms (n: ${stats.getMeasurementsCount})"
      } mkString "\n"
    )

    val statsCleaned = latencyStats map {
      case (host, stats) =>
        val shortHost = host.getAddress.getHostAddress
        shortHost -> Map(
          "count" -> stats.getMeasurementsCount,
          "latency_ms" -> stats.getLatencyScore / 1e6
        )
    }
    println(statsCleaned)

    val extras = Map(
      "server_addr" -> ReservationServer.host,
      "monitor" -> statsCleaned
    )

    val ss = new StringPrintStream()
    metrics.write(ss, extras, configFilter="-")
    tw.Future.value(ss.mkString)
  }

  override def metricsReset(): tw.Future[Unit] = {
    metrics.factory.reset()
    tw.Future.Unit
  }

}

object ReservationCommon {
  def allHosts(cluster: Cluster) =
    cluster.getMetadata.getAllHosts.map { _.getAddress.getHostAddress }

  def thisHost = InetAddress.getLocalHost.getHostAddress
}

object ReservationServer extends {
  override implicit val space = KeySpace("reservations")
} with OwlService {
  import ReservationCommon._

  val host = s"$thisHost:${config.reservations.port}"

  def main(args: Array[String]) {

    val rs = new ReservationServer

    if (thisHost == allHosts(cluster).head) {
      if (config.do_reset) dropKeyspace()
      createKeyspace()
      rs.init()
    }

    val server = Thrift.serveIface(host, rs)

    println("[ready]")
    server.await()
  }
}


class ReservationClient(cluster: Cluster) {
  import ReservationCommon._

  val port = config.reservations.port
  val addrs = allHosts(cluster).map { h => s"$h:$port" }

  def newClient(hosts: String) = {
    val service =
      Thrift.client
          .withLoadBalancer(Balancers.p2cPeakEwma())
          .newServiceIface[th.ReservationService.ServiceIface](hosts, "ipa")

    Thrift.newMethodIface(service)
  }

  println(s"hosts: ${addrs.mkString(",")}")

  val client = newClient(addrs.mkString(","))

  val all = addrs map { addr => newClient(addr) }
}


object ReservationClient extends {
  override implicit val space = KeySpace("reservations")
} with OwlService {

  def main(args: Array[String]) {
    val cass_hosts = cluster.getMetadata.getAllHosts map { _.getAddress.getHostAddress }
    println(s"cassandra hosts: ${cass_hosts.mkString(", ")}")

    val port = config.reservations.port
    val hosts = cass_hosts.map(h => s"$h:$port").mkString(",")

    val service = Thrift.client
        .withSessionPool.maxSize(4)
        .withLoadBalancer(Balancers.aperture())
        .newServiceIface[th.ReservationService.ServiceIface](hosts, "ipa")

    val client = Thrift.newMethodIface(service)

    val t = Table(space.name, "c")
    println(s"create counter")
    client.createCounter(t, 0.05).await()

    println(s"incr counter")
    client.incr(t, 0.id.toString, 1L).await()

    println(s"done")
    sys.exit()
  }
}