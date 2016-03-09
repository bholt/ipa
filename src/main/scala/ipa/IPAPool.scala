package ipa

import java.util.UUID

import com.websudos.phantom.dsl._
import owl._
import owl.Util._
import com.twitter.{util => tw}
import com.datastax.driver.core.{ConsistencyLevel => CLevel}

import scala.concurrent.Future
import scala.util.Try

abstract class IPAPool(name: String)(implicit imps: CommonImplicits)
    extends DataType(imps)
{ self: IPAPool.Ops =>

  case class Info(key: UUID, capacity: Long)

  class InfoTable extends CassandraTable[InfoTable, Info] {

    object key extends UUIDColumn(this) with PartitionKey[UUID]
    object capacity extends LongColumn(this) with StaticColumn[Long]

    override val tableName = name
    override def fromRow(r: Row) = Info(key(r), capacity(r))
  }

  val info = new InfoTable
  val entries = new IPASet[UUID](name+"_set") with IPASet.WeakOps[UUID]
  val counts = new IPACounter(name+"_count") with IPACounter.WeakOps

  def createTwitter(): tw.Future[Unit] = {
    tw.Future.join(
      DataType.createWithMetadata(name, info, meta.toString),
      entries.createTwitter(),
      counts.createTwitter()
    ).unit
  }

  override def create(): Future[Unit] =
    createTwitter().asScala

  override def truncate(): Future[Unit] =
    Seq(
      info.truncate().future().unit,
      entries.truncate(),
      counts.truncate()
    ).bundle.unit

  object prepared {
    private val (t, k, cap) = (info.tableName, info.key.name, info.capacity.name)

    lazy val init: (UUID, Long) => (CLevel) => BoundOp[Unit] = {
      val ps = session.prepare(s"UPDATE $t SET $cap = ? WHERE $k = ?")
      (key: UUID, capacity: Long) => ps.bindWith(capacity, key)(_ => ())
    }

    lazy val capacity: (UUID) => (CLevel) => BoundOp[Long] = {
      val ps = session.prepare(s"SELECT $cap FROM $t WHERE $k = ?")
      (key: UUID) => ps.bindWith(key)(_.first.map(info.capacity(_)).getOrElse(0L))
    }

  }

}

object IPAPool {

  trait Ops { self: IPAPool =>
    type IPAType[T] <: Inconsistent[T]

    def init(key: UUID, capacity: Long): Future[Unit]
    def take(key: UUID, capacity: Option[Long]): Future[Option[UUID]]
    def remaining(key: UUID): Future[Long]
  }

  trait StrongOps extends Ops { self: IPAPool =>

    override def init(key: UUID, capacity: Long): Future[Unit] = {
      prepared.init(key, capacity)(CLevel.ALL).execAsScala()
    }

    // FIXME: this isn't atomic, so doesn't actually prevent over-taking
    // also, this is going to be pretty slow because of all the round-trips
    override def take(key: UUID, capacity: Option[Long]): Future[Option[UUID]] = {
      {
        for {
          cap <- capacity map {
            Future(_)
          } getOrElse {
            prepared.capacity(key)(CLevel.ONE).execAsScala()
          }
          taken <- counts.prepared.read(key)(CLevel.QUORUM).execAsScala()
          if taken < cap
          v = UUID.randomUUID()
          _ <- entries.prepared.add(key, v)(CLevel.QUORUM).execAsScala()
          _ <- counts.prepared.incr(key, 1)(CLevel.QUORUM).execAsScala()
        } yield {
          Some(v)
        }
      } recover {
        case _ => None
      }
    }

    override def remaining(key: UUID) = {
      counts.prepared.read(key)(CLevel.QUORUM).execAsScala()
    }

  }
}
