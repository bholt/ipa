package ipa

import java.net.InetAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.UUID
import java.util.function.Function

import com.datastax.driver.core.{Cluster, ConsistencyLevel => CLevel}
import com.twitter.finagle.{Deadline, Thrift, ThriftMux}
import com.twitter.finagle.loadbalancer.Balancers
import com.twitter.finagle.util.{DefaultTimer, HashedWheelTimer}
import com.twitter.util._
import com.twitter.{util => tw}
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import ipa.IPACounter.WeakOps
import ipa.thrift._
import ipa.{thrift => th}
import org.joda.time.DateTime
import owl.Connector._
import owl.Consistency._
import owl.Util._
import owl.{Connector, OwlService, Timestamped, Tolerance}

import scala.collection.JavaConversions._
import scala.collection.{concurrent, mutable}
import scala.concurrent.blocking
import scala.util.{Failure, Success, Try}



class ReservationServer(implicit imps: CommonImplicits) extends th.ReservationService[tw.Future] {
  import imps._

  val boundedCounters = new ConcurrentHashMap[Table, BoundedCounter]

  override def boundedCounter(t: Table, op: BoundedCounterOp): Future[CounterResult] = {
    implicit val space = KeySpace(t.space)
    implicit val imps = CommonImplicits()
    val bc = boundedCounters.computeIfAbsent(t, new Function[Table, BoundedCounter] {
      def apply(t: Table) = {
        BoundedCounter.fromName(t.name) recoverWith {
          case e: Throwable =>
            Console.err.println(s"Error getting BoundedCounter by name: $t")
            Failure(e)
        } get
      }
    })
    bc.handle(op)
  }

  val counters = new ConcurrentHashMap[Table, IPACounter with IPACounter.ErrorTolerance]

  override def counter(t: Table, op: BoundedCounterOp): Future[CounterResult] = {
    import IPACounter._
    implicit val space = KeySpace(t.space)
    implicit val imps = CommonImplicits()
    val c = counters.computeIfAbsent(t, new Function[Table, IPACounter with ErrorTolerance] {
      override def apply(t: Table): IPACounter with ErrorTolerance = {
        IPACounter.fromName(t.name).recoverWith {
          case e: Throwable =>
            Console.err.println(s"Error getting BoundedCounter by name: $t")
            Failure(e)
        }.get.asInstanceOf[IPACounter with ErrorTolerance]
      }
    })
    c.server.handle(op)
  }

//  override def create(tbl: Table, datatype: Datatype, tolerance: Double): Future[Unit] = ???

  override def metricsJson(): tw.Future[String] = {
    Console.err.println(s"metricsJson()")
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
    Console.err.println("# reset metrics")
    metrics.factory.reset()
    tw.Future.Unit
  }

}

object ReservationCommon {

  def replicaAddrs(cluster: Cluster) = cluster.getMetadata.getAllHosts.map(_.getAddress)

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
    }

    val monitor: Monitor = new Monitor {
      def handle(e: Throwable): Boolean = {
        Console.err.println(s"!! monitor got error: ${e.getMessage}")
        e.printStackTrace()
        false
      }
    }

    val server =
      ThriftMux.server
        .withMonitor(monitor)
        .serveIface(host, rs)

    println("[ready]")
    server.await()
  }
}


class ReservationClient(cluster: Cluster) {
  import ReservationCommon._

  val port = config.reservations.port
  val addrs = replicaAddrs(cluster) map { addr =>
    addr -> s"${addr.getHostAddress}:$port"
  } toMap

  def newClient(hosts: String): ReservationService = {
    val service =
      ThriftMux.client
          .withLoadBalancer(Balancers.p2cPeakEwma())
          .newServiceIface[th.ReservationService.ServiceIface](hosts, "ipa")

    ThriftMux.newMethodIface(service)
  }

  println(s"hosts: ${addrs.values.mkString(",")}")

  val client = newClient(addrs.values.mkString(","))

  val clients = addrs map { case (addr,host) => addr -> newClient(host) }

  def resetMetrics() = clients.values.map(_.metricsReset()).bundle().unit.await()

  def fetchMetrics() = {
    clients.values
        .map { client => client.metricsJson() }
        .map { f => f map { j => json.readValue(j, classOf[Map[String,Any]]) } }
        .bundle()
        .await()
        .reduce(combine)
  }
}
