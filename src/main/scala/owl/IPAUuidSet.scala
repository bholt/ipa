package owl

import com.datastax.driver.core.{ConsistencyLevel, Row}
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.builder.primitives.Primitive
import com.websudos.phantom.column.PrimitiveColumn
import com.websudos.phantom.dsl._
import com.websudos.phantom.keys.PartitionKey
import nl.grons.metrics.scala.Timer

import scala.concurrent._
import scala.collection.JavaConversions._
import scala.math.Ordering.Implicits._

import Connector.config
import Util._

import scala.concurrent.duration.FiniteDuration

class IPAUuidSet(val name: String)(implicit val session: Session, val space: KeySpace, val cassandraOpMetric: Timer) extends TableGenerator {
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

  class Handle(key: K, cons: ConsistencyLevel) {

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

  def apply(key: K) = new Handle(key, ConsistencyLevel.ALL)
}

trait LatencyBound extends IPAUuidSet {
  def latencyBound: FiniteDuration

  def rush[T](latencyBound: FiniteDuration)(op: () => Future[Inconsistent[T]]): Future[Rushed[T]] = {
    val deadline = latencyBound.fromNow

    val ops =
      Seq(ConsistencyLevel.ALL, ConsistencyLevel.ONE) map { c =>
        op() map { r => Rushed(r.get, c) }
      }

    ops.firstCompleted flatMap { r1 =>
      val timeRemaining = deadline.timeLeft
      if (r1.consistency == ConsistencyLevel.ALL ||
          timeRemaining < config.assumed_latency) {
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

  class RushHandle(v: V) extends Handle(v, ConsistencyLevel.ALL) {

    override def size(): Future[Rushed[Int]] =
      rush(latencyBound){ () => super.size() }

    override def contains(v: V): Future[Rushed[Boolean]] =
      rush(latencyBound){ () => super.contains(v) }

    override def get(limit: Int = 0): Future[Rushed[Iterator[V]]] =
      rush(latencyBound){ () => super.get(limit) }
  }

  override def apply(key: K) = new RushHandle(key)
}
