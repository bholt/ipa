package ipa

import com.codahale.metrics.Histogram
import com.datastax.driver.core._
import owl.{IPAMetrics, MetricCell}
import owl.Util._

import scala.collection.mutable

/**
  * LatencyTracker using DropWizard metrics.
  */
class MetricsLatencyTracker(metrics: IPAMetrics) extends LatencyTracker {

  val strong = metrics.create.histogram("tracker_strong")
  val others = metrics.create.histogram("tracker_weak")

  def metric(cons: ConsistencyLevel) = cons match {
    case ConsistencyLevel.ALL => strong
    case _ => others
  }

  override def update(host: Host, stmt: Statement, ex: Exception, lat: Long): Unit = {
    metric(stmt.getConsistencyLevel).update(lat)
  }

  def predict(cons: ConsistencyLevel): Double = {
    metric(cons).getSnapshot.getMean
  }

  override def onUnregister(cluster: Cluster): Unit = {}
  override def onRegister(cluster: Cluster): Unit = {}
}
