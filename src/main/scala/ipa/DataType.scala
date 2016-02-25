package ipa

import owl.Util._
import com.datastax.driver.core.{ConsistencyLevel => CLevel}
import com.websudos.phantom.dsl._
import nl.grons.metrics.scala.Timer
import owl.Connector.config
import owl.{IPAMetrics, Inconsistent, Rushed, TableGenerator}
import com.twitter.{util => tw}
import ipa.thrift.ReservationService

import scala.concurrent._
import scala.concurrent.duration.FiniteDuration
import scala.math.Ordering.Implicits._

case class CommonImplicits(implicit val session: Session, val space: KeySpace, val metrics: IPAMetrics, val reservations: ReservationService[tw.Future])

abstract class DataType(imps: CommonImplicits) extends TableGenerator {
  def name: String

  implicit val session = imps.session
  implicit val space = imps.space
  implicit val metrics = imps.metrics
  implicit val reservations = imps.reservations
}

trait RushImpl { this: DataType =>
  def rush[T](latencyBound: FiniteDuration)(op: CLevel => Future[T]): Future[Rushed[T]] = {
    val deadline = latencyBound.fromNow

    val ops =
      Seq(ConsistencyLevel.ALL, ConsistencyLevel.ONE) map { c =>
        op(c) map { r => Rushed(r, c) }
      }

    ops.firstCompleted flatMap { r1 =>
      val timeRemaining = deadline.timeLeft
      if (r1.consistency == ConsistencyLevel.ALL ||
          timeRemaining < config.assumed_latency) {
        if (deadline.isOverdue()) metrics.missedDeadlines.mark()
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
