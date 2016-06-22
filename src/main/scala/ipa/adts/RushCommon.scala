package ipa.adts

import com.datastax.driver.core.{ConsistencyLevel => CLevel}
import com.twitter.util.{Return, Throw, Duration => TwDuration, Future => TwFuture}
import com.websudos.phantom.dsl._
import ipa.Connector._
import ipa.Util._
import ipa.types.Consistency._
import ipa.types._

import scala.concurrent._
import scala.concurrent.duration.FiniteDuration
import scala.math.Ordering.Implicits._


/**
  * Created by bholt on 6/22/16.
  */
trait RushCommon { this: DataType =>

  lazy val strongThresholded = metrics.create.counter("rush_strong_thresholded")
  lazy val rush_both = metrics.create.counter("rush_both")

  def rush[T](bound: TwDuration)(op: CLevel => TwFuture[T]): TwFuture[Rushed[T]] = {
    val thresholdNanos = bound.inNanoseconds * 1.5
    val prediction = metrics.tracker.predict(Strong)
    if (prediction > thresholdNanos) {
      strongThresholded += 1
      op(Weak) map { Rushed(_, Weak) }
    } else {
      rush_both += 1
      val deadline = bound.fromNow
      val ops = Seq(Strong, Weak) map { c => op(c) map { r => Rushed(r, c) } }
      TwFuture.select(ops) flatMap {
        case (Return(r1), remaining) =>
          if (r1.consistency == Strong || deadline.sinceNow < config.assumed_latency) {
            if (deadline.sinceNow < TwDuration.Zero) metrics.missedDeadlines.mark()
            TwFuture(r1)
          } else {
            TwFuture.select(remaining).raiseWithin(deadline.sinceNow) map {
              case (Return(r2), _) =>
                r1 max r2
              case (Throw(e), _) =>
                Console.err.println(s"Error in rush: second try errored out? $r1 ${e.getMessage}")
                r1
            } handle { case e =>
              // fallback
              r1
            }
          }
        case (Throw(e), remaining) =>
          val msg = s"Error inside first attempt: ${e.getMessage}"
          Console.err.println(msg)
          TwFuture.exception(e)
      }
    }
  }

  def rush[T](latencyBound: FiniteDuration)(op: CLevel => Future[T]): Future[Rushed[T]] = {
    val thresholdNanos = latencyBound.toNanos * 1.5
    val prediction = metrics.tracker.predict(Strong)

    if (prediction > thresholdNanos) {
      strongThresholded += 1
      // then don't even try Strong consistency
      op(Weak) map { Rushed(_, Weak) }
    } else {
      val ops =
        Seq(Strong, Weak) map { c =>
          op(c) map { r => Rushed(r, c) }
        }
      val deadline = latencyBound.fromNow
      ops.firstCompleted flatMap { r1 =>
        val timeRemaining = deadline.timeLeft
        if (r1.consistency == Strong ||
            timeRemaining < config.assumed_latency) {
          if (deadline.isOverdue()) metrics.missedDeadlines.mark()
          Future(r1)
        } else {
          // make sure it finishes within the deadline
          val fallback = Future {
            blocking {
              Thread.sleep(timeRemaining.toMillis)
            }
            r1
          }
          (ops.filterNot(_.isCompleted) :+ fallback)
              .firstCompleted
              .map { r2 => r1 max r2 } // return the higher-consistency one
        }
      }
    }
  }
}
