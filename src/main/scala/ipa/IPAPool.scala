package ipa

import java.util.UUID

import com.websudos.phantom.dsl._
import owl._
import owl.Util._
import com.twitter.{util => tw}
import com.datastax.driver.core.{ConsistencyLevel => CLevel}

import scala.concurrent.Future
import scala.util.Try

abstract class IPAPool(val name: String)(implicit val imps: CommonImplicits)
    extends DataType(imps)
{ self: IPAPool.Ops =>

  class Handle(key: UUID, capacity: Option[Long]) {
    def init(cap: Long = -1) = {
      val c = if (cap >= 0) cap else capacity.get
      self.init(key, c) map {
        _ => new Handle(key, Some(c))
      }
    }
    def take() = self.take(key, capacity)
    def remaining() = self.remaining(key, capacity)
  }

  def apply(key: UUID, capacity: Option[Long] = None) = new Handle(key, capacity)

}

object IPAPool {

  trait Ops { self: IPAPool =>
    type IPAType[T] <: Inconsistent[T]

    def init(key: UUID, capacity: Long): Future[Unit]
    def take(key: UUID, capacity: Option[Long]): Future[IPAType[Option[UUID]]]
    def remaining(key: UUID, capacity: Option[Long]): Future[IPAType[Long]]
  }

  trait SetPlusCounter extends Ops { self: IPAPool =>

    case class Info(key: UUID, capacity: Long)

    class InfoTable extends CassandraTable[InfoTable, Info] {

      object key extends UUIDColumn(this) with PartitionKey[UUID]
      object capacity extends LongColumn(this)

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
      private val (k, cap) = (info.key.name, info.capacity.name)
      private val t = s"${space.name}.${info.tableName}"

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

  trait StrongOps extends SetPlusCounter {
    self: IPAPool =>

    type IPAType[T] = Consistent[T]

    override def init(key: UUID, capacity: Long): Future[Unit] = {
      prepared.init(key, capacity)(CLevel.ALL).execAsScala()
    }

    // FIXME: this isn't atomic, so doesn't actually prevent over-taking
    // also, this is going to be pretty slow because of all the round-trips
    override def take(key: UUID, capacity: Option[Long]): Future[IPAType[Option[UUID]]] = {
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
          Consistent(Option(v))
        }
      } recover {
        case _ => Consistent[Option[UUID]](None)
      }
    }

    override def remaining(key: UUID, capacity: Option[Long]): Future[IPAType[Long]] = {
      for {
        cap <- capacity map {
          Future(_)
        } getOrElse {
          prepared.capacity(key)(CLevel.ONE).execAsScala()
        }
        taken <- counts.prepared.read(key)(CLevel.QUORUM).execAsScala()
      } yield {
        Consistent(cap - taken)
      }
    }
    
  }
}
