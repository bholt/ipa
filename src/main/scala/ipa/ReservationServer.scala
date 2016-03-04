package ipa

import java.net.InetAddress
import java.util.UUID

import scala.concurrent.duration.{Deadline, Duration, NANOSECONDS}
import scala.util.{Failure, Success, Try}
import com.datastax.driver.core.{Cluster, ConsistencyLevel => CLevel}
import com.twitter.finagle.loadbalancer.Balancers
import com.twitter.finagle.Thrift
import com.twitter.util._
import com.twitter.{util => tw}
import com.websudos.phantom.connectors.KeySpace
import ipa.Counter.WeakOps
import ipa.thrift.{ReservationException, Table}
import ipa.{thrift => th}
import owl.Connector.config
import owl.Util._
import owl.{Connector, OwlService, Tolerance}

import scala.collection.concurrent
import scala.collection.JavaConversions._
import scala.collection.mutable
import owl.Consistency._

case class Timestamped[T](value: T, time: Long = System.nanoTime) {
  def expired: Boolean = (System.nanoTime - time) > config.lease.periodNanos
  def get: Option[T] = if (!expired) Some(value) else None
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

    val cached_reads = metrics.create.counter("cached_reads")

    val races = metrics.create.counter("races")

    val latencyWeakWrite   = metrics.create.timer("weak_write_latency")
    val latencyStrongWrite = metrics.create.timer("strong_write_latency")
    val latencyWeakRead    = metrics.create.timer("weak_read_latency")
    val latencyStrongRead  = metrics.create.timer("strong_read_latency")
  }

  case class Entry(
      table: Counter,
      space: KeySpace,
      tolerance: Tolerance
  ) {
    val reservations = new mutable.HashMap[UUID, Reservation]

    def reservation(key: UUID) = reservations.get(key)

    def reservation(key: UUID, lastRead: => Timestamped[Long]) =
      reservations.getOrElseUpdate(key, Reservation(this, lastRead))

    def refresh(key: UUID): tw.Future[Reservation] = {
      // read strong always performs read repair so this suffices to ensure that everyone is up-to-date and we can start using our reservations again
      // TODO: verify read repair is happening
      // TODO: check reservations table rather than assuming `allocated` is constant
      val rt = System.nanoTime // conservative read time
      table.readTwitter(Strong)(key).instrument(m.latencyStrongRead) map { v =>
        val vt = Timestamped(v, rt)
        val res = reservation(key, vt)
        // now that we've synchronized with everyone, we get our tokens back
        res.update(vt, tolerance)
        res
      }
    }

  }

  class Reservation {
    var lastRead: Timestamped[Long] = Timestamped(0L, 0L)
    var total: Long = 0L      // tokens allocated globally (currently assumed to be the max possible given the error tolerance)
    var allocated: Long = 0L  // tokens allocated to this replica locally
    var available: Long = 0L // local tokens remaining


    def update(_lastRead: Timestamped[Long], tol: Tolerance): Unit = {
      lastRead = _lastRead

      // TODO: assuming that maximum possible are allocated
      total = tol.delta(lastRead.value)

      // TODO: assuming that each replica gets lower bound of its fair share
      allocated = total / session.nreplicas
      available = allocated

      // println(s">> update => $this")
    }

    def used = allocated - available
    def delta = total - used

    override def toString = s"Reservation(read: $lastRead, total: $total, alloc: $allocated, avail: $available)"
  }

  object Reservation {
    def apply(entry: Entry, lastRead: Timestamped[Long]): Reservation = {
      val r = new Reservation()
      r.update(lastRead, entry.tolerance)
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
        Counter.fromName(t.name) map {
          case tbl: Counter with Counter.ErrorTolerance =>
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
    val counter = new Counter(t.name) with WeakOps
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
          res.lastRead.get map { v => // if cached read not expired
            m.cached_reads += 1
            // return the cached value
            tw.Future.value(th.IntervalLong(v - res.delta, v + res.delta))
          }
        } getOrElse { // if no reservation or expired read:
          // read from local cassandra replica
          val rt = System.nanoTime
          e.table.readTwitter(Weak)(key)
              .instrument(m.latencyWeakRead)
              .instrument() map { v =>
            val vt = Timestamped(v, rt)
            // first time, create new Reservation and store in hashmap
            val res = e.reservation(key, vt)
            th.IntervalLong(v - res.delta, v + res.delta)
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
        // may need to get the latest value of the counter if we haven't created a reservation for this record yet
        val get = { () =>
          val rt = System.nanoTime
          e.table.readTwitter(CLevel.ONE)(key)
              .instrument(m.latencyStrongRead)
              .instrument()
              .map(Timestamped(_, rt))
              .await()
        }
        val res = e.reservation(key, get())

        // execute the increment on durable store
        val exec = { cons: CLevel =>
          val timer = if (cons == Strong) m.latencyStrongWrite
                      else                m.latencyWeakWrite
          e.table.incrTwitter(cons)(key, n).instrument(timer).instrument()
        }

        if (n >= res.allocated) {
          m.outOfBounds += 1
          // cannot execute within error bounds, so must wait until all have received it
          exec(CLevel.ALL) flatMap { _ => e.refresh(key).unit } map { v =>
            // make sure we wait until lease period has definitely expired
            Thread.sleep(config.lease.periodMillis)
            v
          }
        } else {
          if (res.available < n) {
            m.refreshes += 1
            // println(s">> incr($n) need to refresh: $res")
            // need to get more
            e.refresh(key) flatMap { _ =>
              if (res.available >= n) { // enough tokens available:
                res.available -= n
                exec(CLevel.ONE)
              } else { // race where some other op took the token
                m.races += 1
                // rather than possibly iterating again, just give up and wait
                exec(CLevel.ALL)
              }
            }
          } else { // enough tokens available:
            m.immediates += 1
            res.available -= n
            exec(CLevel.ONE) // just needs to be fault-tolerant
          }
        }
      case Failure(e) =>
        tw.Future.exception(e)
    }
  }

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
    if (thisHost == allHosts(cluster).head) {
      if (config.do_reset) dropKeyspace()
      createKeyspace()
    }

    val server = Thrift.serveIface(host, new ReservationServer)

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