package ipa

import java.util.UUID

import com.websudos.phantom.dsl.{UUID, _}
import owl._
import owl.Util._
import com.twitter.{util => tw}
import com.datastax.driver.core.{ConsistencyLevel => CLevel}

import scala.concurrent.Future

abstract class IPAPool(name: String)(implicit imps: CommonImplicits)
    extends DataType(imps)
{ self: IPAPool.Ops =>

  case class Entry(key: UUID, value: UUID, capacity: Option[Long])

  class EntryTable extends CassandraTable[EntryTable, Entry] {
    object key extends UUIDColumn(this) with PartitionKey[UUID]
    object value extends UUIDColumn(this) with PrimaryKey[UUID]
    object capacity extends LongColumn(this) with StaticColumn[Long]
    override val tableName = name
    override def fromRow(r: Row) = Entry(key(r), value(r), capacity.optional(r).toOption)
  }

  case class Count(key: UUID, taken: Long)

  class CountTable extends CassandraTable[CountTable, Count] {
    object key extends UUIDColumn(this) with PartitionKey[UUID]
    object taken extends CounterColumn(this)

    override val tableName = name + "_count"
    override def fromRow(r: Row) = Count(key(r), taken(r))
  }

  val entries = new EntryTable
  val counts = new CountTable

  def createTwitter(): tw.Future[Unit] = {
    DataType.createWithMetadata(name, entries, meta.toString)
  }

  override def create(): Future[Unit] =
    createTwitter().asScala

  override def truncate(): Future[Unit] =
    entries.truncate.future().unit


  object prepared {
    val (k, v, t) = (entries.key.name, entries.value.name, table.fullname)

    lazy val add: (UUID, UUID) => (CLevel) => BoundOp[Unit] = {
      val ps = session.prepare(s"INSERT INTO $t ($k, $v) VALUES (?, ?)")
      (key: UUID, value: UUID) => ps.bindWith(key, value)(_ => ())
    }

    lazy val remove: (UUID, UUID) => (CLevel) => BoundOp[Unit] = {
      val ps = session.prepare(s"DELETE FROM $t WHERE $k=? AND $v=?")
      (key: UUID, value: UUID) => ps.bindWith(key, value)(_ => ())
    }

    lazy val contains: (UUID, UUID) => (CLevel) => BoundOp[Boolean] = {
      val ps = session.prepare(s"SELECT $v FROM $t WHERE $k=? AND $v=? LIMIT 1")
      (key: UUID, value: UUID) => ps.bindWith(key, value)(rs => rs.one() != null)
    }

    lazy val size: (UUID) => (CLevel) => BoundOp[Long] = {
      val ps = session.prepare(s"SELECT COUNT(*) FROM $t WHERE $k = ?")
      (key: UUID) => ps.bindWith(key)(_.first.map(_.get(0, classOf[Long])).getOrElse(0L))
    }

    private val (ctk, ct) = (counts.key.name, counts.taken.name)
    private val cap = entries.capacity.name

    lazy val init: (UUID, Long) => (CLevel) => BoundOp[Unit] = {
      val ps = session.prepare(s"UPDATE ${entries.tableName} SET $cap=? WHERE $k=?")
      (key: UUID, capacity: Long) => ps.bindWith(capacity, key)(_ => ())
    }

    
  }

}

object IPAPool {

  trait Ops { self: IPAPool =>
    type IPAType[T] <: Inconsistent[T]

    def init(key: UUID, capacity: Long): Future[Unit]
    def take(key: UUID): Future[UUID]
    def remaining(key: UUID): Future[Long]
  }

  trait WeakOps extends Ops { self: IPAPool =>

    val base = new IPASet[UUID](name + "_set") with IPASet.WeakOps[UUID]

    override def init(key: UUID, capacity: Long) = {

    }
  }
}
