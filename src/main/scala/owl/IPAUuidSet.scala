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
  import imps._

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

  object prepared {
    val (k, v, t) = (entryTable.ekey.name, entryTable.evalue.name, s"${space.name}.$name")

    lazy val add: (UUID, V) => (ConsistencyLevel) => BoundOp[Unit] = {
      val ps = session.prepare(s"INSERT INTO $t ($k, $v) VALUES (?, ?)")
      (key: UUID, value: V) => ps.bindWith(key, value)(_ => ())
    }

    lazy val remove: (UUID, V) => (ConsistencyLevel) => BoundOp[Unit] = {
      val ps = session.prepare(s"DELETE FROM $t WHERE $k=? AND $v=?")
      (key: UUID, value: V) => ps.bindWith(key, value)(_ => ())
    }

    lazy val contains: (UUID, V) => (ConsistencyLevel) => BoundOp[Boolean] = {
      val ps = session.prepare(s"SELECT $v FROM $t WHERE $k=? AND $v=? LIMIT 1")
      (key: UUID, value: V) => ps.bindWith(key, value)(rs => rs.one() != null)
    }

    lazy val size: (UUID) => (ConsistencyLevel) => BoundOp[Long] = {
      val ps = session.prepare(s"SELECT COUNT(*) FROM $t WHERE $k = ?")
      (key: UUID) => ps.bindWith(key)(_.first.map(_.get(0, classOf[Long])).getOrElse(0L))
    }
  }

  class HandleBase(key: K, cons: ConsistencyLevel) {

    def contains(value: V): Future[Inconsistent[Boolean]] = {
      prepared.contains(key, value)(cons).execAsScala().map(Inconsistent(_))
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
      prepared.add(key, value)(cons).execAsScala()
    }

    def remove(value: V): Future[Unit] = {
      prepared.remove(key, value)(cons).execAsScala()
    }

    def size(): Future[Inconsistent[Int]] = {
      prepared.size(key)(cons).execAsScala().map(i => Inconsistent(i.toInt))
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
