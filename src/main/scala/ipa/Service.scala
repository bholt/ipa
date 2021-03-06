package ipa

import java.io.PrintStream
import java.util.concurrent.TimeUnit

import com.codahale.metrics
import com.codahale.metrics._
import com.datastax.driver.core.Cluster
import ipa.Connector.config
import ipa.Util._
import ipa.adts.CommonImplicits

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.{Future, blocking}

// for Vector.sample
import scala.language.postfixOps

trait TableGenerator {
  def create(): Future[Unit]
  def truncate(): Future[Unit]
}

class MetricCell[T](var metric: T)
object MetricCell { def apply[T](metric: T) = new MetricCell[T](metric) }

class IPAMetrics(output: scala.collection.Map[String,AnyRef], cluster: Cluster) {

  val registry = new metrics.MetricRegistry

  val tracker = new MetricsLatencyTracker(this)
  cluster.register(tracker)

  object factory {
    val timers = mutable.HashMap[String,MetricCell[Timer]]()
    val counters = mutable.HashMap[String,MetricCell[Counter]]()
    val meters = mutable.HashMap[String,MetricCell[Meter]]()
    val histograms = mutable.HashMap[String,MetricCell[Histogram]]()

    def timer(name: String) =
      timers.getOrElseUpdate(name, MetricCell(registry.timer(name)))

    def counter(name: String) =
      counters.getOrElseUpdate(name, MetricCell(registry.counter(name)))

    def meter(name: String) =
      meters.getOrElseUpdate(name, MetricCell(registry.meter(name)))

    def histogram(name: String) =
      histograms.getOrElseUpdate(name, MetricCell(registry.histogram(name)))

    def reset() = {
      registry.removeMatching(MetricFilter.ALL)
      for ((name, cell) <- timers)     cell.metric = registry.timer(name)
      for ((name, cell) <- counters)   cell.metric = registry.counter(name)
      for ((name, cell) <- meters)     cell.metric = registry.meter(name)
      for ((name, cell) <- histograms) cell.metric = registry.histogram(name)
    }
  }

  def create = factory

  lazy val cassandraOpLatency = create.timer("cass_op_latency")
  lazy val missedDeadlines = create.meter("missed_deadlines")

  import Connector.json

  def write(out: PrintStream, extras: Map[String,AnyRef] = Map(), configFilter: String = "ipa") = {
    val mConfig = config.c.root().withOnlyKey(configFilter).unwrapped()
    val mOutput = output map { case (k,v) => s"out_$k" -> v }
    val mMetrics = json.readValue(json.writeValueAsString(registry), classOf[java.util.Map[String,Object]])
    val writer = json.writerWithDefaultPrettyPrinter()
    out.println(writer.writeValueAsString(mConfig ++ mMetrics ++ mOutput ++ extras))
  }

  def dump()(implicit reservations: ReservationClient): Unit = {

    // collect metrics from reservation servers

    println("# Metrics".bold)
    ConsoleReporter.forRegistry(registry)
        .convertRatesTo(TimeUnit.SECONDS)
        .build()
        .report()
    
    // dump metrics to stderr (for experiments script to parse)
    if (config.output_json) {
      write(Console.err, Map("res" -> reservations.fetchMetrics()))
    }
    println("###############################")
  }

}

trait IPAService extends Connector {

  implicit val metrics = new IPAMetrics(output, cluster)

  implicit val imps = CommonImplicits()

  ///////////////////////
  // Other tables
  ///////////////////////

  object service {

    def resetKeyspace(): Unit = {
      val tmpSession = blocking { cluster.connect() }
      if (config.do_reset) {
        println(s"# Resetting keyspace '${space.name}'")
        blocking {
          tmpSession.execute(s"DROP KEYSPACE IF EXISTS ${space.name}")
        }
      }

      createKeyspace(tmpSession)
    }

  }
}
