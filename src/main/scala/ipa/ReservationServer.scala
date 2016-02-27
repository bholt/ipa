package ipa

import java.net.InetAddress
import java.util.UUID

import scala.util.{Failure, Success, Try}
import com.datastax.driver.core.{Cluster, ConsistencyLevel => CLevel}
import com.twitter.finagle.loadbalancer.Balancers
import com.twitter.finagle.{Thrift, ThriftMux}
import com.twitter.util._
import com.twitter.{util => tw}
import com.websudos.phantom.connectors.KeySpace
import ipa.Counter.WeakOps
import ipa.thrift.{ReservationException, Table}
import ipa.{thrift => th}
import owl.Connector.config
import owl.Util._
import owl.{OwlService, Tolerance}

import scala.collection.JavaConversions._
import scala.collection.mutable

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
  }

  case class Entry(
      table: Counter,
      space: KeySpace,
      tolerance: Tolerance
  ) {
    val reservations = new mutable.HashMap[UUID, Reservation]

    def reservation(key: UUID, lastRead: => Long) =
      reservations.getOrElseUpdate(key, Reservation(this, lastRead))

    def refresh(key: UUID): tw.Future[Reservation] = {
      // read strong always performs read repair so this suffices to ensure that everyone is up-to-date and we can start using our reservations again
      // TODO: verify read repair is happening
      // TODO: check reservations table rather than assuming `allocated` is constant
      table.readTwitter(CLevel.ALL)(key) map { v =>
        val res = reservation(key, v)

        // now that we've synchronized with everyone, we get our tokens back
        res.update(v, tolerance)
        res
      }
    }

  }

  class Reservation {
    var lastRead: Long = 0L
    var total: Long = 0L      // tokens allocated globally (currently assumed to be the max possible given the error tolerance)
    var allocated: Long = 0L  // tokens allocated to this replica locally
    var available: Long = 0L // local tokens remaining

    def update(_lastRead: Long, tol: Tolerance): Unit = {
      lastRead = _lastRead

      // TODO: assuming that maximum possible are allocated
      total = tol.delta(lastRead)

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
    def apply(entry: Entry, lastRead: Long): Reservation = {
      val r = new Reservation()
      r.update(lastRead, entry.tolerance)
      r
    }
  }

  val tables = new mutable.HashMap[Table, Entry]

  def table(t: Table): Try[Entry] = {
    Try(tables(t)) recoverWith { case _ =>
      implicit val space = KeySpace(t.space)
      implicit val imps = CommonImplicits()
      Counter.fromName(t.name) flatMap {
        case tbl: Counter with Counter.ErrorTolerance =>
          val e = Entry(tbl, space, tbl.tolerance)
          tables += (t -> e)
          Success(e)
        case tbl =>
          //println(s"Counter without error tolerance: $name")
          //Entry(tbl, space, Tolerance(0))
          Failure(ReservationException(s"counter without error tolerance: $t"))
      }
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
        e.table.readTwitter(CLevel.ONE)(key).instrument() map { raw =>
          // first time, create new Reservation and store in hashmap
          val res = e.reservation(key, raw)
          // println(s">> raw = $raw, res.delta = ${res.delta}; $res")
          th.IntervalLong(raw - res.delta, raw + res.delta)
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
        val get = { () => e.table.readTwitter(CLevel.ALL)(key).instrument().await() }
        val res = e.reservation(key, get())

        // consume
        val exec = { cons: CLevel => e.table.incrTwitter(cons)(key, n).instrument() }

        if (n > res.allocated) {
          m.outOfBounds += 1
          // println(s">> incr($n) outside error bounds $res, executing with strong consistency")
          // cannot execute within error bounds, so must execute with strong consistency
          exec(CLevel.ALL) flatMap { _ => e.refresh(key).unit }
        } else {
          if (res.available < n) {
            m.refreshes += 1
            // println(s">> incr($n) need to refresh: $res")
            // need to get more
            e.refresh(key) flatMap { _ =>
              assert(res.available >= n)
              res.available -= n
              // println(s">> incr($n) => $res")
              exec(CLevel.ONE)
            }
          } else {
            m.immediates += 1
            res.available -= n
            // println(s">> incr($n) => $res")
            exec(CLevel.ONE)
          }
        }
      case Failure(e) =>
        tw.Future.exception(e)
    }
  }

  override def metricsJson(): tw.Future[String] = {
    val ss = new StringPrintStream()
    metrics.write(ss, Map("server_addr" -> ReservationServer.host), configFilter="-")
    tw.Future.value(ss.mkString)
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
  val client = newClient(addrs.toList.reverse.mkString(","))
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