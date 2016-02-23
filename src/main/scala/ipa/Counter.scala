package ipa

import java.util.UUID

import com.datastax.driver.core.{Row, ConsistencyLevel => CLevel}
import com.twitter.{util => tw}
import com.websudos.phantom.builder.query.{ExecutableQuery, ExecutableStatement, SelectQuery}
import com.websudos.phantom.dsl._
import com.websudos.phantom.keys.PartitionKey
import ipa.thrift.IntervalLong
import nl.grons.metrics.scala.Timer
import owl._

import scala.concurrent.Future
import owl.Util._

import scala.concurrent.duration.FiniteDuration
import owl.Conversions._

object Counter {

  object Ops {

    trait Incr {
      def incr(key: UUID, by: Long): Future[Unit]
    }

    trait Read {
      type ReadType

      def read(key: UUID): Future[ReadType]
    }

  }

  trait WeakOps extends Ops.Incr with Ops.Read { base: Counter =>
    type ReadType = Inconsistent[Long]
    override def read(key: UUID) =
      base.read(CLevel.ONE)(key).map(Inconsistent(_))
    override def incr(key: UUID, by: Long) = base.incr(CLevel.ONE)(key, by)
  }

  trait StrongOps extends Ops.Incr with Ops.Read { base: Counter =>
    type ReadType = Long
    override def read(key: UUID) = base.read(CLevel.ALL)(key)
    override def incr(key: UUID, by: Long) = base.incr(CLevel.ALL)(key, by)
  }

  trait LatencyBound extends Ops.Incr with Ops.Read with RushImpl {
    base: Counter =>

    def bound: FiniteDuration

    type ReadType = Rushed[Long]

    override def read(key: UUID) =
      rush(bound){ c: CLevel => base.read(c)(key) }

    override def incr(key: UUID, by: Long) =
      base.incr(CLevel.ONE)(key, by)
  }

  trait ErrorTolerance extends Ops.Incr with Ops.Read {
    base: Counter =>

    def tolerance: Tolerance

    override def create(): Future[Unit] = {
      base.create() flatMap { _ =>
        reservations.createCounter(name, space.name, tolerance.error).asScala
      }
    }

    type ReadType = Interval[Long]

    override def incr(key: UUID, by: Long): Future[Unit] = {
      reservations.incr(name, key.toString, by).asScala
    }

    override def read(key: UUID): Future[Interval[Long]] = {
      reservations.readInterval(name, key.toString)
          .map(v => v: Interval[Long])
          .asScala
    }
  }

}

class Counter(val name: String)(implicit imps: CommonImplicits) extends DataType(imps) {
  self: Counter.Ops.Incr with Counter.Ops.Read =>

  case class Count(key: UUID, count: Long)
  class CountTable extends CassandraTable[CountTable, Count] {
    object ekey extends UUIDColumn(this) with PartitionKey[UUID]
    object ecount extends CounterColumn(this)
    override val tableName = name
    override def fromRow(r: Row) = Count(ekey(r), ecount(r))
  }

  val tbl = new CountTable

  override def create(): Future[Unit] =
    tbl.create.ifNotExists.future().unit

  override def truncate(): Future[Unit] =
    tbl.truncate.future().unit

  class Handle(key: UUID) {
    def incr(by: Long = 1L): Future[Unit] = self.incr(key, by)
    def read(): Future[ReadType] = self.read(key)
  }
  def apply(key: UUID) = new Handle(key)


  def incrStmt(c: CLevel)(key: UUID, by: Long): ExecutableStatement = {
    tbl.update()
        .where(_.ekey eqs key)
        .modify(_.ecount += by)
        .consistencyLevel_=(c)
  }

  def incr(c: CLevel)(key: UUID, by: Long) =
    incrStmt(c)(key, by).future().instrument().unit

  def incrTwitter(c: CLevel)(key: UUID, by: Long): tw.Future[Unit] =
    incrStmt(c)(key, by).execute().instrument().unit

  def readStmt(c: CLevel)(key: UUID) = {
    tbl.select(_.ecount)
        .where(_.ekey eqs key)
        .consistencyLevel_=(c)
  }

  def read(c: CLevel)(key: UUID) =
    readStmt(c)(key).one().instrument().map(_.getOrElse(0L))

  def readTwitter(c: CLevel)(key: UUID): tw.Future[Long] =
    readStmt(c)(key).get().instrument().map(_.getOrElse(0L))

}
