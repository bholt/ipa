package ipa

import com.datastax.driver.core.{Row, ConsistencyLevel => CLevel}
import com.twitter.{util => tw}
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.builder.primitives.Primitive
import com.websudos.phantom.column.PrimitiveColumn
import com.websudos.phantom.dsl.{UUID, _}
import com.websudos.phantom.keys.PartitionKey
import owl.Util._
import owl.{Interval, _}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds

object IPASet {

  trait Ops[V] { self: IPASet[V] =>
    type IPAType[T] <: Inconsistent[T]

    def add(key: K, value: V): Future[Unit]
    def remove(key: K, value: V): Future[Unit]
    def contains(key: K, value: V): Future[IPAType[Boolean]]
    def size(key: K): Future[IPAType[Long]]
  }

  trait WriteOps[V] extends Ops[V] { self: IPASet[V] =>
    def writeLevel: CLevel

    override def add(key: K, value: V): Future[Unit] =
      _add(key, value)(writeLevel)

    override def remove(key: K, value: V): Future[Unit] =
      _remove(key, value)(writeLevel)

  }

  import Consistency._

  trait WeakOps[V] extends WriteOps[V] { self: IPASet[V] =>

    type IPAType[T] = Inconsistent[T]
    override val writeLevel = Strong
    val readLevel = Weak

    override def contains(key: K, value: V): Future[IPAType[Boolean]] = {
      _contains(key, value)(readLevel).map(Inconsistent(_))
    }

    override def size(key: K): Future[IPAType[Long]] = {
      _size(key)(readLevel).map(Inconsistent(_))
    }
  }

  trait StrongOps[V] extends WriteOps[V] { self: IPASet[V] =>

    type IPAType[T] = Consistent[T]
    override val writeLevel = Strong
    val readLevel = Strong

    override def contains(key: K, value: V): Future[IPAType[Boolean]] = {
      _contains(key, value)(readLevel).map(Consistent(_))
    }

    override def size(key: K): Future[IPAType[Long]] = {
      _size(key)(readLevel).map(Consistent(_))
    }
  }

  trait LatencyBound[V] extends WriteOps[V] with RushImpl {
    base: IPASet[V] =>

    def bound: FiniteDuration

    type IPAType[T] = Rushed[T]
    override val writeLevel = Strong

    override def contains(key: K, value: V) =
      rush(bound)(_contains(key, value))

    override def size(key: K) =
      rush(bound)(_size(key))
  }

  trait ErrorTolerance[V] extends Ops[V] { base: IPASet[V] =>

    def tolerance: Tolerance

    override def meta = Metadata(Some(tolerance))

    override def create(): Future[Unit] = {
      createTwitter() flatMap { _ =>
        reservations.client.createCounter(table, tolerance.error)
      } asScala
    }

    type IPAType[T] = Interval[T]

    override def add(key: K, value: V): Future[Unit] = ???
    override def remove(key: K, value: V): Future[Unit] = ???
    override def contains(key: K, value: V): Future[IPAType[Boolean]] = ???
    override def size(key: K): Future[IPAType[Long]] = ???
  }
}

abstract class IPASet[V:Primitive](val name: String)(implicit imps: CommonImplicits) extends DataType(imps) {
  self: IPASet.Ops[V] =>

  type K = UUID

  case class Entry(key: K, value: V)
  class EntryTable extends CassandraTable[EntryTable, Entry] {
    object key extends PrimitiveColumn[EntryTable, Entry, K](this) with PartitionKey[K]
    object value extends PrimitiveColumn[EntryTable, Entry, V](this) with PrimaryKey[V]
    override val tableName = name
    override def fromRow(r: Row) = Entry(key(r), value(r))
  }

  val tbl = new EntryTable

  def createTwitter(): tw.Future[Unit] = {
    DataType.createWithMetadata(name, tbl, meta.toString)
  }

  override def create(): Future[Unit] =
    createTwitter().asScala

  override def truncate(): Future[Unit] =
    tbl.truncate.future().unit

  class Handle(key: K) {
    def add(value: V) = self.add(key, value)
    def remove(value: V) = self.remove(key, value)
    def contains(value: V) = self.contains(key, value)
    def size() = self.size(key)
  }

  def apply(key: UUID) = new Handle(key)

  object prepared {
    val (k, v, t) = (tbl.key.name, tbl.value.name, table.fullname)

    lazy val add: (UUID, V) => (CLevel) => BoundOp[Unit] = {
      val ps = session.prepare(s"INSERT INTO $t ($k, $v) VALUES (?, ?)")
      (key: UUID, value: V) => ps.bindWith(key, value)(_ => ())
    }

    lazy val remove: (UUID, V) => (CLevel) => BoundOp[Unit] = {
      val ps = session.prepare(s"DELETE FROM $t WHERE $k=? AND $v=?")
      (key: UUID, value: V) => ps.bindWith(key, value)(_ => ())
    }

    lazy val contains: (UUID, V) => (CLevel) => BoundOp[Boolean] = {
      val ps = session.prepare(s"SELECT $v FROM $t WHERE $k=? AND $v=? LIMIT 1")
      (key: UUID, value: V) => ps.bindWith(key, value)(rs => rs.one() != null)
    }

    lazy val size: (UUID) => (CLevel) => BoundOp[Long] = {
      val ps = session.prepare(s"SELECT COUNT(*) FROM $t WHERE $k = ?")
      (key: UUID) => ps.bindWith(key)(_.first.map(_.get(0, classOf[Long])).getOrElse(0L))
    }
  }


  def _add(key: K, value: V)(c: CLevel): Future[Unit] =
    prepared.add(key, value)(c).execAsScala()

  def _remove(key: K, value: V)(c: CLevel): Future[Unit] =
    prepared.remove(key, value)(c).execAsScala()

  def _contains(k: K, v: V)(c: CLevel): Future[Boolean] =
    prepared.contains(k, v)(c).execAsScala()

  def _size(k: K)(c: CLevel): Future[Long] =
    prepared.size(k)(c).execAsScala()

}
