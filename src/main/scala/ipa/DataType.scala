package ipa

import owl.Util._
import com.datastax.driver.core.{ConsistencyLevel => CLevel}
import com.websudos.phantom.builder.query.ExecutableStatement
import com.websudos.phantom.dsl._
import nl.grons.metrics.scala.Timer
import owl.Connector.config
import owl.{IPAMetrics, Inconsistent, Rushed, TableGenerator}

import scala.concurrent._
import scala.concurrent.duration.FiniteDuration
import scala.math.Ordering.Implicits._

case class CommonImplicits(implicit val session: Session, val space: KeySpace, val cassandraOpMetric: Timer, val ipa_metrics: IPAMetrics)

abstract class DataType(imps: CommonImplicits) extends TableGenerator {
  def name: String

  implicit val session = imps.session
  implicit val space = imps.space
  implicit val cassandraOpMetric = imps.cassandraOpMetric
  implicit val ipa_metrics = imps.ipa_metrics
}

trait RushImpl { this: DataType =>
  def rush[T](latencyBound: FiniteDuration)(op: CLevel => Future[Inconsistent[T]]): Future[Rushed[T]] = {
    val deadline = latencyBound.fromNow

    val ops =
      Seq(ConsistencyLevel.ALL, ConsistencyLevel.ONE) map { c =>
        op(c) map { r => Rushed(r.get, c) }
      }

    ops.firstCompleted flatMap { r1 =>
      val timeRemaining = deadline.timeLeft
      if (r1.consistency == ConsistencyLevel.ALL ||
          timeRemaining < config.assumed_latency) {
        if (deadline.isOverdue()) ipa_metrics.missedDeadlines.mark()
        Future(r1)
      } else {
        // make sure it finishes within the deadline
        val fallback = Future {
          blocking { Thread.sleep(timeRemaining.toMillis) }
          r1
        }
        (ops.filterNot(_.isCompleted) :+ fallback)
            .firstCompleted
            .map { r2 => r1 max r2 } // return the higher-consistency one
      }
    }
  }
}
