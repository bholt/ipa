package ipa

import java.net.InetAddress
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Function

import com.datastax.driver.core.Cluster
import com.twitter.finagle.ThriftMux
import com.twitter.finagle.loadbalancer.Balancers
import com.twitter.util._
import com.websudos.phantom.connectors.KeySpace
import ipa.Connector._
import ipa.Util._
import ipa.adts._
import ipa.thrift._
import ipa.{thrift => th}

import scala.collection.JavaConversions._
import scala.collection.immutable.IndexedSeq
import scala.util.{Failure, Random}



class ReservationServer(implicit imps: CommonImplicits) extends th.ReservationService[Future] {
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
            Console.err.println(s"Error getting IPACounter by name: $t")
            Failure(e)
        }.get.asInstanceOf[IPACounter with ErrorTolerance]
      }
    })
    c.server.handle(op)
  }

  type IPASetWithErrorBound = IPASet[UUID] with IPASet.ErrorBound[UUID]
  val ipasets = new ConcurrentHashMap[Table, IPASetWithErrorBound]

  override def ipaSet(t: Table, op: SetOp): Future[SetResult] = {
    implicit val space = KeySpace(t.space)
    implicit val imps = CommonImplicits()
    val c = ipasets.computeIfAbsent(t, new Function[Table,IPASetWithErrorBound] {
      override def apply(t: Table): IPASetWithErrorBound = {
        IPASet.fromName[UUID](t.name).recoverWith {
          case e: Throwable =>
            Console.err.println(s"Error getting IPASet by name: $t")
            Failure(e)
        }.get.asInstanceOf[IPASetWithErrorBound]
      }
    })
    c.server.handle(op)
  }

  override def metricsJson(): Future[String] = {
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
    Future(ss.mkString)
  }

  override def metricsReset(): Future[Unit] = {
    Console.err.println("# reset metrics")
    metrics.factory.reset()
    Future.Unit
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
} with IPAService {
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
      ThriftMux.client
          .withLoadBalancer(Balancers.p2cPeakEwma())
          .build[ReservationService.MethodPerEndpoint](hosts)
  }

  val latencies: IndexedSeq[(InetAddress, LatencyMeasurer)] =
    addrs map { case (addr,_) => addr -> new RunningAverageLatency } toIndexedSeq

  def get: (InetAddress, LatencyMeasurer) = {
    Random.nextDouble() match {
      case v if v < 0.05 => latencies.sample
      case _ => latencies.minBy(_._2.get)
    }
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
