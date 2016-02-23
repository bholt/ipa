package ipa

import java.net.InetAddress

import com.datastax.driver.core.{ConsistencyLevel => CLevel}
import com.twitter.finagle.Thrift
import com.twitter.finagle.loadbalancer.Balancers
import com.twitter.util.Await
import com.twitter.{util => tw}
import com.websudos.phantom.connectors.KeySpace
import ipa.Counter.WeakOps
import ipa.{thrift => th}
import owl.Util._
import owl.{Connector, OwlService, Tolerance}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global

class ReservationServer(implicit imps: CommonImplicits) extends th.ReservationService[tw.Future] {
  import imps._

  case class Entry(
      table: Counter with WeakOps,
      space: KeySpace,
      tolerance: Tolerance
  )

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

  override def readInterval(name: String, key: String): tw.Future[th.IntervalLong] = {
    val e = tables(name)
    e.table.readTwitter(CLevel.ONE)(key.toUUID) map { iv =>
      // TODO: implement this for real rather than pretending
      val raw = iv
      val tol = e.tolerance.error
      val epsilon = (raw * tol).toLong
      println(s"raw: $raw, epsilon: $epsilon".cyan)
      th.IntervalLong(raw - epsilon, raw + epsilon)
    }
  }

  override def incr(name: String, key: String, by: Long): tw.Future[Unit] = {
    val e = tables(name)
    e.table.incrTwitter(CLevel.ONE)(key.toUUID, by)
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

    Await.result(server)
//    val clientService = Thrift.newServiceIface[th..ServiceIface](host, "ipa")
//
//    val client = Thrift.newMethodIface(clientService)
//    client.log("hello", 1) map { println(_) } await()

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