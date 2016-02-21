package ipa

import java.util.UUID

import com.datastax.driver.core.{ConsistencyLevel => CLevel}
import com.datastax.driver.core.Session
import com.twitter.finagle.{ServiceFactory, Thrift}
import com.twitter.util.Await
import com.twitter.{util => tw}
import com.websudos.phantom.connectors.KeySpace
import ipa.{thrift => th}
import nl.grons.metrics.scala.Timer
import owl.{Connector, IPAMetrics, OwlService, Tolerance}
import owl.Util._

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class ReservationService(implicit imps: CommonImplicits) extends th.ReservationService[tw.Future] {

  val tables = new mutable.HashMap[String, Counter with Counter.ErrorTolerance]

  /**
    * Initialize new UuidSet
    * TODO: make generic version
    */
  override def createUuidset(name: String, sizeTolerance: Double): tw.Future[Unit] = ???

  /** Initialize new Counter table. */
  override def createCounter(table: String, error: Double): tw.Future[Unit] = {
    val counter = new Counter(table)
        with Counter.ErrorTolerance { override val tolerance = Tolerance(error) }
    tables += (table -> counter)
    counter.create().asTwitter
  }

  override def readInterval(name: String, key: String): tw.Future[th.IntervalLong] = {
    val counter = tables(name)
    counter.read(CLevel.ONE)(key.toUUID) map { iv =>
      // TODO: implement this for real rather than pretending
      val raw = iv.get
      val tol = counter.tolerance.error
      val epsilon = (raw/tol).toLong
      th.IntervalLong(raw - epsilon, raw + epsilon)
    } asTwitter // TODO: use Twitter Future directly rather than converting
  }

  override def incr(name: String, key: String, by: Long): tw.Future[Unit] = {
    val counter = tables(name)
    counter.incr(CLevel.ONE)(key.toUUID, by).asTwitter
  }
}

object ReservationService extends OwlService {
  override implicit val space = KeySpace(Connector.config.keyspace)

  val host = "localhost:14007"

  def main(args: Array[String]) {

    createKeyspace()

    val server = Thrift.serveIface(host, new ReservationService)

    Await.result(server)
//    val clientService = Thrift.newServiceIface[th..ServiceIface](host, "ipa")
//
//    val client = Thrift.newMethodIface(clientService)
//    client.log("hello", 1) map { println(_) } await()

  }
}

object ReservationClient extends OwlService {
  override implicit val space = KeySpace(Connector.config.keyspace)

  def main(args: Array[String]) {
    val cass_hosts = cluster.getMetadata.getAllHosts
    println(s"cassandra hosts: ${cass_hosts.mkString(",")}")

    val client = Thrift.newMethodIface(
      Thrift.newServiceIface[th.ReservationService.ServiceIface](ReservationService.host, "ipa")
    )

    val tbl = "c"
    client.createCounter(tbl, 0.05).await()

    client.incr(tbl, 0.id.toString, 1L).await()

    sys.exit()
  }
}