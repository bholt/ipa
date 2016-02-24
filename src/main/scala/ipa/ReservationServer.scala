package ipa

import java.net.InetAddress
import java.util.UUID

import com.datastax.driver.core.{ConsistencyLevel => CLevel}
import com.twitter.finagle.Thrift
import com.twitter.finagle.loadbalancer.Balancers
import com.twitter.{util => tw}
import com.websudos.phantom.connectors.KeySpace
import ipa.Counter.WeakOps
import ipa.{thrift => th}
import owl.Util._
import owl.{Connector, OwlService, Tolerance}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}

class ReservationServer(implicit imps: CommonImplicits) extends th.ReservationService[tw.Future] {
  import imps._

  case class Entry(
      table: Counter with WeakOps,
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
        res.available = res.allocated
        res.lastRead = v
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
    }

    def used = available - allocated
    def delta = total - used

  }

  object Reservation {
    def apply(entry: Entry, lastRead: Long): Reservation = {
      val r = new Reservation()
      r.update(lastRead, entry.tolerance)
      r
    }
  }

  val tables = new mutable.HashMap[String, Entry]

  /**
    * Initialize new UuidSet
    * TODO: make generic version
    */
  override def createUuidset(name: String, sizeTolerance: Double): tw.Future[Unit] = ???

  /** Initialize new Counter table. */
  override def createCounter(table: String, keyspace: String, error: Double): tw.Future[Unit] = {
    implicit val space = KeySpace(keyspace)
    implicit val imps = CommonImplicits()
    val counter = new Counter(table) with WeakOps
    tables += (table -> Entry(counter, space, Tolerance(error)))
    tw.Future.Unit
  }

  override def readInterval(name: String, keyStr: String): tw.Future[th.IntervalLong] = {
    val key = keyStr.toUUID
    val e = tables(name)
    e.table.readTwitter(CLevel.ONE)(key) map { raw =>
      // first time, create new Reservation and store in hashmap
      val res = e.reservation(key, raw)
      th.IntervalLong(raw - res.delta, raw + res.delta)
    }
  }

  override def incr(name: String, keyStr: String, n: Long): tw.Future[Unit] = {
    val key = keyStr.toUUID
    val e = tables(name)
    // may need to get the latest value of the counter if we haven't created a reservation for this record yet
    val get = { () => e.table.readTwitter(CLevel.ALL)(key).await() }
    val res = e.reservation(key, get())

    // consume
    val exec = { cons: CLevel => e.table.incrTwitter(cons)(key, n) }

    if (n > res.allocated) {
      // cannot execute within error bounds, so must execute with strong consistency
      exec(CLevel.ALL)
    } else {
      if (res.available < n) {
        // need to get more
        e.refresh(key) flatMap { _ =>
          assert(res.available >= n)
          res.available -= n
          exec(CLevel.ONE)
        }
      } else {
        res.available -= n
        exec(CLevel.ONE)
      }
    }
  }
}

object ReservationServer extends {
  override implicit val space = KeySpace("reservations")
} with OwlService {

  val host = s"${InetAddress.getLocalHost.getHostAddress}:${config.reservations.port}"

  def main(args: Array[String]) {
    if (config.do_reset) dropKeyspace()
    createKeyspace()

    val server = Thrift.serveIface(host, new ReservationServer)

    println("[ready]")
    server.await()
  }
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

    val tbl = "c"
    println(s"create counter")
    client.createCounter(tbl, "reservations", 0.05).await()

    println(s"incr counter")
    client.incr(tbl, 0.id.toString, 1L).await()

    println(s"done")
    sys.exit()
  }
}