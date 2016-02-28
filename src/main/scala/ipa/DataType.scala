package ipa

import owl.Util._
import com.datastax.driver.core.{ConsistencyLevel => CLevel}
import com.websudos.phantom.dsl._
import owl.Connector.config
import owl.{IPAMetrics, Rushed, TableGenerator}

import scala.concurrent._
import scala.concurrent.duration.FiniteDuration
import scala.math.Ordering.Implicits._
import scala.util.Try

case class CommonImplicits(implicit val session: Session, val space: KeySpace, val metrics: IPAMetrics, val reservations: ReservationClient)

abstract class DataType(imps: CommonImplicits) extends TableGenerator {
  def name: String

  /* metadata to store in the Cassandra table properties */
  def meta: Map[String,Any] = Map()

  implicit val session = imps.session
  implicit val space = imps.space
  implicit val metrics = imps.metrics
  implicit val reservations = imps.reservations
}

object DataType {
  def lookupMetadata(name: String)(implicit imps: CommonImplicits): Try[Map[String, Any]] = {
    import imps._
    val query = s"SELECT comment FROM system.schema_columnfamilies WHERE keyspace_name = '${space.name}' AND columnfamily_name = '$name'"
    Try {
      val row = blocking { session.execute(query).one() }
      val text = row.get("comment", classOf[String])
      metrics.json.readValue(text, classOf[Map[String, Any]])
    }
  }
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
