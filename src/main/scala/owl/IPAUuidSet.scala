package owl

import com.codahale.metrics.MetricRegistry
import com.datastax.driver.core.{ConsistencyLevel, Row}
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.column.PrimitiveColumn
import com.websudos.phantom.dsl._
import com.websudos.phantom.keys.PartitionKey

import scala.concurrent._
import scala.collection.JavaConversions._
import scala.math.Ordering.Implicits._
import java.util.UUID

import Connector.config
import Util._
import ipa.CommonImplicits

import scala.concurrent.duration.FiniteDuration

class IPAUuidSet(val name: String)(implicit val imps: CommonImplicits) extends TableGenerator {
  import imps.metrics
  import imps.session
  import imps.space
  import imps.reservations

  type K = UUID
  type V = UUID

  case class Entry(key: K, value: V)
  class EntryTable extends CassandraTable[EntryTable, Entry] {
    object ekey extends PrimitiveColumn[EntryTable, Entry, K](this) with PartitionKey[K]
    object evalue extends PrimitiveColumn[EntryTable, Entry, V](this) with PrimaryKey[V]
    override val tableName = name
    override def fromRow(r: Row) = Entry(ekey(r), evalue(r))
  }

  val entryTable = new EntryTable

  override def create(): Future[Unit] =
    entryTable.create.ifNotExists.future().unit

  override def truncate(): Future[Unit] =
    entryTable.truncate.future().unit

  class HandleBase(key: K, cons: ConsistencyLevel) {

    def contains(value: V): Future[Inconsistent[Boolean]] = {
      entryTable.select(_.evalue)
          .consistencyLevel_=(cons)
          .where(_.ekey eqs key)
          .and(_.evalue eqs value)
          .one()
          .instrument()
          .map(o => Inconsistent(o.isDefined))
    }

    def get(limit: Int = 0): Future[Inconsistent[Iterator[V]]] = {
      val q = entryTable.select
          .consistencyLevel_=(cons)
          .where(_.ekey eqs key)

      val qlim = if (limit > 0) q.limit(limit) else q

      qlim.future().instrument().map { results =>
        results.iterator() map { row =>
          entryTable.fromRow(row).value
        }
      }.map(Inconsistent(_))
    }

    def add(value: V): Future[Unit] = {
      entryTable.insert()
          .consistencyLevel_=(cons)
          .value(_.ekey, key)
          .value(_.evalue, value)
          .future()
          .instrument()
          .unit
    }

    def remove(value: V): Future[Unit] = {
      entryTable.delete()
          .consistencyLevel_=(cons)
          .where(_.ekey eqs key)
          .and(_.evalue eqs value)
          .future()
          .instrument()
          .unit
    }

    def size(): Future[Inconsistent[Int]] = {
      entryTable.select.count()
          .consistencyLevel_=(cons)
          .where(_.ekey eqs key)
          .one()
          .map(o => Inconsistent(o.getOrElse(0l).toInt))
          .instrument()
    }


  }

  def apply(key: K) = new HandleBase(key, ConsistencyLevel.ALL)
}

trait ConsistencyBound extends IPAUuidSet {
  def consistencyLevel: ConsistencyLevel

  override def apply(key: K) = new HandleBase(key, consistencyLevel)
}


trait LatencyBound extends IPAUuidSet {
  import imps._
  def latencyBound: FiniteDuration

  def rush[T](latencyBound: FiniteDuration)(op: ConsistencyLevel => Future[Inconsistent[T]]): Future[Rushed[T]] = {
    val deadline = latencyBound.fromNow

    val ops =
      Seq(ConsistencyLevel.ALL, ConsistencyLevel.ONE) map { c =>
        op(c) map { r => Rushed(r.get, c) }
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

  class Handle(key: K) extends HandleBase(key, ConsistencyLevel.ALL) {

    override def size(): Future[Rushed[Int]] =
      rush(latencyBound){ c: ConsistencyLevel => new HandleBase(key, c).size() }

    override def contains(v: V): Future[Rushed[Boolean]] =
      rush(latencyBound){ c: ConsistencyLevel => new HandleBase(key, c).contains(v) }

    override def get(limit: Int = 0): Future[Rushed[Iterator[V]]] =
      rush(latencyBound){ c: ConsistencyLevel => new HandleBase(key, c).get(limit) }
  }

  override def apply(key: K) = new Handle(key)
}
