package owl

import java.util.concurrent.TimeoutException

import com.datastax.driver.core.{Row, ConsistencyLevel}
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.builder.primitives.Primitive
import com.websudos.phantom.column.{CounterColumn, SetColumn, PrimitiveColumn}
import com.websudos.phantom.dsl._
import com.websudos.phantom.keys.PartitionKey
import nl.grons.metrics.scala.Timer

import scala.collection.JavaConversions._
import scala.concurrent.{Await, Future}

import Util._

import scala.concurrent._
import scala.concurrent.duration.{Deadline, FiniteDuration, Duration}
import scala.util.Try

abstract class IPASet[K, V] extends TableGenerator {
  def consistency: ConsistencyLevel

  def create(): Future[Unit]
  def truncate(): Future[Unit]
  def contains(key: K, value: V): Future[Boolean]
  def add(key: K, value: V): Future[Unit]
  def remove(key: K, value: V): Future[Unit]

  def size(key: K): Future[Int]

  /**
    * Local handle to a Set in storage; can be used like a Set
    *
    * @param key  identifier of this Set instance in storage
    */
  class Handle(key: K) {
    def contains(value: V) = IPASet.this.contains(key, value)
    def add(value: V) = IPASet.this.add(key, value)
    def remove(value: V) = IPASet.this.remove(key, value)
    def size() = IPASet.this.size(key)
  }

  def apply(key: K) = new Handle(key)
}

/**
  * IPASet implementation using a Cassandra Set collection column
  */
class IPASetImplCollection[K, V](val name: String, val consistency: ConsistencyLevel)(implicit val evK: Primitive[K], val evV: Primitive[V], val session: Session, val space: KeySpace, val cassandraOpMetric: Timer) extends IPASet[K, V] {

  case class Entry(key: K, value: Set[V])

  class EntryTable extends CassandraTable[EntryTable, Entry] {
    object ekey extends PrimitiveColumn[EntryTable, Entry, K](this) with PartitionKey[K]
    object evalue extends SetColumn[EntryTable, Entry, V](this) with Index[Set[V]]
    override val tableName = name
    override def fromRow(r: Row) = Entry(ekey(r), evalue(r))
  }

  val entryTable = new EntryTable

  override def create(): Future[Unit] = { entryTable.create.ifNotExists.future().unit }
  override def truncate(): Future[Unit] = { entryTable.truncate().future().unit }

  override def contains(key: K, value: V): Future[Boolean] = {
    entryTable.select.count()
        .consistencyLevel_=(consistency)
        .where(_.ekey eqs key)
        .and(_.evalue contains value)
        .one()
        .instrument()
        .map { ctOpt => ctOpt.exists(_ > 0) }
  }

  override def add(key: K, value: V): Future[Unit] = {
    entryTable.update()
        .consistencyLevel_=(consistency)
        .where(_.ekey eqs key)
        .modify(_.evalue.add(value))
        .future()
        .instrument()
        .unit
  }

  override def remove(key: K, value: V): Future[Unit] = {
    entryTable.update()
        .consistencyLevel_=(consistency)
        .where(_.ekey eqs key)
        .modify(_.evalue.remove(value))
        .future()
        .instrument()
        .unit
  }

  override def size(key: K): Future[Int] = {
    entryTable.select(_.evalue)
        .consistencyLevel_=(consistency)
        .where(_.ekey eqs key)
        .one()
        .instrument()
        .map { _.map(set => set.size).getOrElse(0) }
  }

  def get(key: K): Future[Set[V]] = {
    entryTable.select(_.evalue)
        .consistencyLevel_=(consistency)
        .where(_.ekey eqs key)
        .one()
        .map(_.getOrElse(Set[V]()))
  }

  def random(key: K): Future[V] = {
    get(key).map(_.toIndexedSeq.sample)
  }

  class Handle(key: K) extends super.Handle(key) {
    def get() = IPASetImplCollection.this.get(key)
    def random() = IPASetImplCollection.this.random(key)
  }
  override def apply(key: K) = new Handle(key)
}

class IPASetImplPlain[K, V](val name: String, val consistency: ConsistencyLevel)(implicit val evK: Primitive[K], val evV: Primitive[V], val session: Session, val space: KeySpace, val cassandraOpMetric: Timer) extends IPASet[K, V] {

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

  override def contains(key: K, value: V): Future[Boolean] = {
    entryTable.select(_.evalue)
        .consistencyLevel_=(consistency)
        .where(_.ekey eqs key)
        .and(_.evalue eqs value)
        .one()
        .instrument()
        .map(_.isDefined)
  }

  def get(key: K, limit: Int = 0): Future[Iterator[V]] = {
    val q = entryTable.select
        .consistencyLevel_=(consistency)
        .where(_.ekey eqs key)

    val qlim = if (limit > 0) q.limit(limit) else q

    qlim.future().instrument().map { results =>
      results.iterator() map { row =>
        entryTable.fromRow(row).value
      }
    }
  }

  override def add(key: K, value: V): Future[Unit] = {
    entryTable.insert()
        .consistencyLevel_=(consistency)
        .value(_.ekey, key)
        .value(_.evalue, value)
        .future()
        .instrument()
        .unit
  }

  override def remove(key: K, value: V): Future[Unit] = {
    entryTable.delete()
        .consistencyLevel_=(consistency)
        .where(_.ekey eqs key)
        .and(_.evalue eqs value)
        .future()
        .instrument()
        .unit
  }

  override def size(key: K): Future[Int] = {
    entryTable.select.count()
        .consistencyLevel_=(consistency)
        .where(_.ekey eqs key)
        .one()
        .map(_.getOrElse(0l).toInt)
        .instrument()
  }

  override def apply(key: K) = new Handle(key) {
    def get(limit: Int = 0): Future[Iterator[V]] =
      IPASetImplPlain.this.get(key, limit)
  }
}

class IPASetImplWithCounter[K, V](val name: String, val consistency: ConsistencyLevel)(implicit val evK: Primitive[K], val evV: Primitive[V], val session: Session, val space: KeySpace, val cassandraOpMetric: Timer) extends IPASet[K, V] {

  case class Entry(key: K, value: V)
  class EntryTable extends CassandraTable[EntryTable, Entry] {
    object ekey extends PrimitiveColumn[EntryTable, Entry, K](this) with PartitionKey[K]
    object evalue extends PrimitiveColumn[EntryTable, Entry, V](this) with PrimaryKey[V]
    override val tableName = name
    override def fromRow(r: Row) = Entry(ekey(r), evalue(r))
  }

  case class Count(key: K, count: Long)
  class CountTable extends CassandraTable[CountTable, Count] {
    object ekey extends PrimitiveColumn[CountTable, Count, K](this) with PartitionKey[K]
    object ecount extends CounterColumn(this)
    override val tableName = name + "Count"
    override def fromRow(r: Row) = Count(ekey(r), ecount(r))
  }

  val entryTable = new EntryTable
  val countTable = new CountTable

  def create(): Future[Unit] = {
    Seq(entryTable, countTable).map(_.create.ifNotExists.future()).bundle.unit
  }

  def truncate(): Future[Unit] = {
    Seq(entryTable, countTable).map(_.truncate().future()).bundle.unit
  }

  override def contains(key: K, value: V): Future[Boolean] = {
    entryTable.select(_.evalue)
        .consistencyLevel_=(consistency)
        .where(_.ekey eqs key)
        .and(_.evalue eqs value)
        .one()
        .instrument()
        .map(_.isDefined)
  }

  def get(key: K, limit: Int = 0): Future[Iterator[V]] = {
    val q = entryTable.select
        .consistencyLevel_=(consistency)
        .where(_.ekey eqs key)

    val qlim = if (limit > 0) q.limit(limit) else q

    qlim.future().instrument().map { results =>
      results.iterator() map { row =>
        entryTable.fromRow(row).value
      }
    }
  }

  override def add(key: K, value: V): Future[Unit] = {
    // dlog(s">>> $name($key).add($value)")
    this.contains(key, value) flatMap { dup =>
      if (dup) Future { () }
      else {
        for {
          _ <- countTable.update()
              .consistencyLevel_=(consistency)
              .where(_.ekey eqs key)
              .modify(_.ecount += 1)
              .future()
              .instrument()
          _ <- entryTable.insert()
              .consistencyLevel_=(consistency)
              .value(_.ekey, key)
              .value(_.evalue, value)
              .future()
              .instrument()
        } yield ()
      }
    }
  }

  override def remove(key: K, value: V): Future[Unit] = {
    {
      for {
        removed <- this.contains(key, value)
        _ <- entryTable.delete()
            .consistencyLevel_=(consistency)
            .where(_.ekey eqs key)
            .and(_.evalue eqs value)
            .future()
            .instrument()
        if removed
        _ <- countTable.update()
            .consistencyLevel_=(consistency)
            .where(_.ekey eqs key)
            .modify(_.ecount -= 1)
            .future()
            .instrument()
      } yield ()
    } recover {
      case _ => ()
    }
  }

  def size(key: K): Future[Int] = {
    countTable.select(_.ecount)
        .consistencyLevel_=(consistency)
        .where(_.ekey eqs key)
        .one()
        .map(o => o.getOrElse(0l).toInt)
        .instrument()
  }

  override def apply(key: K) = new Handle(key) {
    def get(limit: Int = 0): Future[Iterator[V]] =
      IPASetImplWithCounter.this.get(key, limit)
  }
}
