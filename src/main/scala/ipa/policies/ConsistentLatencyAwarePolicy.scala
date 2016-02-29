package ipa.policies

import com.datastax.driver.core.policies.LoadBalancingPolicy
import com.datastax.driver.core.{Cluster, Host, LatencyTracker, Statement}
import owl.Consistency

/**
  * Modified `LatencyAwarePolicy` that distinguishes Weak vs Strong operations.
  */
class ConsistentLatencyAwarePolicy(
    child: LoadBalancingPolicy,
    exclusionThreshold: Double,
    scale: Long, retryPeriod: Long,
    updateRate: Long,
    minMeasure: Int
) extends MyLatencyAwarePolicy(child, exclusionThreshold, scale, retryPeriod, updateRate, minMeasure) {

  val tracker = new ConsistentTracker



  class ConsistentTracker extends LatencyTracker {
    val trackerWeak = new Tracker
    val trackerStrong = new Tracker

    override def update(host: Host, stmt: Statement, ex: Exception, lat: Long): Unit = {
      // still update the 'real' latencyTracker, just do ours as well
      latencyTracker.update(host, stmt, ex, lat)
      if (stmt.getConsistencyLevel == Consistency.Weak) {
        trackerWeak.update(host, stmt, ex, lat)
      } else {
        trackerStrong.update(host, stmt, ex, lat)
      }
    }

    override def onUnregister(cluster: Cluster): Unit = { /* nothing to do */ }
    override def onRegister(cluster: Cluster): Unit = { /* nothing to do */ }
  }

}
