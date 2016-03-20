package ipa

import com.codahale.metrics.Histogram
import com.datastax.driver.core.{ConsistencyLevel => CLevel, _}
import owl.{IPAMetrics, MetricCell}
import owl.Util._

import scala.collection.mutable

/**
  * LatencyTracker using DropWizard metrics.
  */
class MetricsLatencyTracker(metrics: IPAMetrics) extends LatencyTracker {

  lazy val all = metrics.create.histogram("tracker_all")
  lazy val quorum = metrics.create.histogram("tracker_quorum")
  lazy val weak = metrics.create.histogram("tracker_weak")
  lazy val other = metrics.create.histogram("tracker_other")

  def metric(cons: CLevel) = cons match {
    case CLevel.ALL => all
    case CLevel.QUORUM | CLevel.LOCAL_QUORUM => quorum
    case CLevel.ONE | CLevel.LOCAL_ONE => weak
    case _ => other
  }

  override def update(host: Host, stmt: Statement, ex: Exception, lat: Long): Unit = {
    metric(stmt.getConsistencyLevel).update(lat)
    metrics.replicasUsed(host.getAddress) += 1
  }

  def predict(cons: CLevel): Double = {
    metric(cons).getSnapshot.getMean
  }

  override def onUnregister(cluster: Cluster): Unit = {}
  override def onRegister(cluster: Cluster): Unit = {}
}
