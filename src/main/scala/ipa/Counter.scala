package ipa

import java.util.UUID

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{BoundStatement, Row, ConsistencyLevel => CLevel}
import com.twitter.{util => tw}
import com.websudos.phantom.dsl._
import com.websudos.phantom.keys.PartitionKey
import ipa.thrift.{ReservationException, Table}
import owl.Conversions._
import owl.Util._
import owl._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

object Counter {
  import Consistency._

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
      base.read(Weak)(key).map(Inconsistent(_))
    override def incr(key: UUID, by: Long) =
      base.incr(Weak)(key, by)
  }

  trait StrongWrite extends Ops.Incr with Ops.Read { base: Counter =>
    type ReadType = Consistent[Long]
    override def read(key: UUID): Future[Consistent[Long]] =
      base.read(Weak)(key) map { Consistent(_) }
    override def incr(key: UUID, by: Long): Future[Unit] =
      base.incr(Strong)(key, by)
  }

  trait StrongRead extends Ops.Incr with Ops.Read { base: Counter =>
    type ReadType = Consistent[Long]
    override def read(key: UUID): Future[Consistent[Long]] =
      base.read(Strong)(key) map { Consistent(_) }
    override def incr(key: UUID, by: Long): Future[Unit] =
      base.incr(Weak)(key, by)
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
    
    private def table: Table = Table(space.name, name)
    override def meta() = Map("tolerance" -> tolerance.error)

    override def create(): Future[Unit] = {
      createTwitter() flatMap { _ =>
        reservations.client.createCounter(table, tolerance.error)
      } asScala
    }

    type ReadType = Interval[Long]
    
    override def incr(key: UUID, by: Long): Future[Unit] = {
      reservations.client.incr(table, key.toString, by).asScala
    }

    override def read(key: UUID): Future[Interval[Long]] = {
      reservations.client.readInterval(table, key.toString)
          .map(v => v: Interval[Long])
          .asScala
    }
  }

  def fromName(name: String)(implicit imps: CommonImplicits): Try[Counter] = {
    DataType.lookupMetadata(name) flatMap { meta =>
      Try(meta("tolerance")) flatMap {
        case tol: Double =>
          val c = new Counter(name)
              with ErrorTolerance { override val tolerance = Tolerance(tol) }
          Success(c)
        case tol =>
          Failure(ReservationException(s"tolerance wasn't a double: $tol"))
      } recoverWith {
        case e: Throwable =>
          Failure(ReservationException(s"unable to get 'tolerance': ${e.getMessage}"))
      }
    } recoverWith {
      case e: Throwable =>
        Failure(ReservationException(s"metadata not found for $name"))
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

  def createTwitter(): tw.Future[Unit] = {
    val metaStr = metrics.json.writeValueAsString(meta)

    DataType.lookupMetadata(name) filter { _ == meta } map {
      _ => tw.Future.Unit
    } recover { case e =>
      println(s">>> (re)creating ${space.name}.$name")
      session.execute(s"DROP TABLE IF EXISTS ${space.name}.$name")
      tbl.create.`with`(comment eqs metaStr).execute().unit
    } get
  }

  override def create(): Future[Unit] =
    createTwitter().asScala

  override def truncate(): Future[Unit] =
    tbl.truncate.future().unit

  class Handle(key: UUID) {
    def incr(by: Long = 1L): Future[Unit] = self.incr(key, by)
    def read(): Future[ReadType] = self.read(key)
  }
  def apply(key: UUID) = new Handle(key)

  lazy val preparedIncr = {
    val key = tbl.ekey.name
    val ct = tbl.ecount.name
    session.prepare(s"UPDATE ${space.name}.$name SET $ct=$ct+? WHERE $key=?")
  }

  def incrStmt(c: CLevel)(key: UUID, by: Long): BoundStatement =
    preparedIncr.setConsistencyLevel(c).bind(by.asInstanceOf[AnyRef], key.asInstanceOf[AnyRef])

  def incr(c: CLevel)(key: UUID, by: Long) =
    executeAsScalaFuture(incrStmt(c)(key,by)).instrument().unit

  def incrTwitter(c: CLevel)(key: UUID, by: Long): tw.Future[Unit] =
    executeAsTwitterFuture(incrStmt(c)(key, by)).instrument().unit

  lazy val preparedRead = {
    val key = tbl.ekey.name
    val ct = tbl.ecount.name
    session.prepare(s"SELECT $ct FROM ${space.name}.$name WHERE $key=?")
  }

  def readStmt(c: CLevel)(key: UUID) =
    preparedRead.setConsistencyLevel(c).bind(key.asInstanceOf[AnyRef])

  def readResult(rs: ResultSet) =
    Option(rs.one()).map(_.get(0, classOf[Long])).getOrElse(0L)

  def read(c: CLevel)(key: UUID) =
    executeAsScalaFuture(readStmt(c)(key)).instrument().map(readResult)

  def readTwitter(c: CLevel)(key: UUID): tw.Future[Long] = {
    executeAsTwitterFuture(readStmt(c)(key)).instrument().map(readResult)
  }

}
