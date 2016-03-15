package ipa

import owl.Util._
import com.datastax.driver.core.{ConsistencyLevel => CLevel}
import com.twitter.util.{Return, Throw, Timer, Await => TwAwait, Duration => TwDuration, Future => TwFuture, Try => TwTry}
import com.websudos.phantom.dsl._
import owl.Connector.config
import owl._
import com.twitter.{util => tw}
import ipa.thrift.{ReservationException, Table}

import scala.concurrent._
import scala.concurrent.duration.FiniteDuration
import scala.math.Ordering.Implicits._
import scala.util.Try

case class CommonImplicits(implicit val session: Session, val space: KeySpace, val metrics: IPAMetrics, val reservations: ReservationClient)

import Connector.json

case class Metadata(bound: Option[Bound] = None) {
  override def toString = bound map { b =>
    json.writeValueAsString(Map("bound" -> b.toString))
  } getOrElse {
    ""
  }
}

object Metadata {
  def fromString(s: String)(implicit imps: CommonImplicits) = {
    val m = json.readValue(s, classOf[Map[String,String]])
    Metadata(m.get("bound") map Bound.fromString)
  }
}

abstract class DataType(imps: CommonImplicits) extends TableGenerator {
  def name: String

  protected def table: Table = Table(space.name, name)

  /* metadata to store in the Cassandra table properties */
  def meta: Metadata = Metadata()

  implicit val session = imps.session
  implicit val space = imps.space
  implicit val metrics = imps.metrics
  implicit val reservations = imps.reservations
}

object DataType {
  def lookupMetadata(name: String)(implicit imps: CommonImplicits): Try[String] = {
    import imps._
    val query = s"SELECT comment FROM system.schema_columnfamilies WHERE keyspace_name = '${space.name}' AND columnfamily_name = '$name'"
    Try {
      val row = blocking { session.execute(query).one() }
      val text = row.get("comment", classOf[String])
      text
    }
  }

  def createWithMetadata[T <: CassandraTable[T, _], E](name: String, tbl: T, metaStr: String)(implicit imps: CommonImplicits): tw.Future[Unit] = {
    import imps._
    DataType.lookupMetadata(name) filter { _ == metaStr } map {
      _ => tw.Future.Unit
    } recover { case e =>
      println(s">>> (re)creating ${space.name}.$name")
      session.execute(s"DROP TABLE IF EXISTS ${space.name}.$name")
      tbl.create.`with`(comment eqs metaStr).execute().unit
    } get
  }
}

trait RushImpl { this: DataType =>
  import Consistency._

  lazy val strongThresholded = metrics.create.counter("rush_strong_thresholded")

  def rush[T](bound: TwDuration)(op: CLevel => TwFuture[T]): TwFuture[Rushed[T]] = {
    val thresholdNanos = bound.inNanoseconds * 1.5
    val prediction = metrics.tracker.predict(Strong)
    if (prediction > thresholdNanos) {
      strongThresholded += 1
      op(Weak) map { Rushed(_, Weak) }
    } else {
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
          TwFuture.exception(ReservationException(msg))
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
