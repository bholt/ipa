package ipa.adts

import com.datastax.driver.core.{Row, ConsistencyLevel => CLevel}
import com.twitter.util.{Throw, Future => TwFuture, Try => TwTry}
import com.twitter.{util => tw}
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.builder.primitives.Primitive
import com.websudos.phantom.column.PrimitiveColumn
import com.websudos.phantom.dsl.{UUID, _}
import com.websudos.phantom.keys.PartitionKey
import ipa.ReservationPool
import ipa.Util._
import ipa.thrift.Primitive._
import ipa.thrift.ReservationException
import ipa.types._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds
import scala.util.Try

object IPASet {

  trait Ops[V] { self: IPASet[V] =>
    type IPAType[T] <: Inconsistent[T]
    type SizeType[T] <: Inconsistent[T]

    def add(key: K, value: V): Future[Unit]
    def remove(key: K, value: V): Future[Unit]
    def contains(key: K, value: V): Future[IPAType[Boolean]]
    def size(key: K): Future[SizeType[Long]]
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
    type SizeType[T] = Inconsistent[T]
    override val writeLevel = Strong
    val readLevel = Weak

    override def contains(key: K, value: V): Future[IPAType[Boolean]] = {
      _contains(key, value)(readLevel).map(Inconsistent(_))
    }

    override def size(key: K): Future[SizeType[Long]] = {
      _size(key)(readLevel).map(Inconsistent(_))
    }
  }

  trait StrongOps[V] extends WriteOps[V] { self: IPASet[V] =>

    type IPAType[T] = Consistent[T]
    type SizeType[T] = Consistent[T]
    override val writeLevel = Strong
    val readLevel = Strong

    override def contains(key: K, value: V): Future[IPAType[Boolean]] = {
      _contains(key, value)(readLevel).map(Consistent(_))
    }

    override def size(key: K): Future[SizeType[Long]] = {
      _size(key)(readLevel).map(Consistent(_))
    }
  }

  trait LatencyBound[V] extends WriteOps[V] with RushImpl {
    base: IPASet[V] =>

    def bound: FiniteDuration

    type IPAType[T] = Rushed[T]
    type SizeType[T] = Rushed[T]
    override val writeLevel = Strong

    override def contains(key: K, value: V) =
      rush(bound)(_contains(key, value))

    override def size(key: K) =
      rush(bound)(_size(key))
  }

  trait ErrorBound[V] extends Ops[V] { base: IPASet[V] =>
    import Conversions._
    import ipa.thrift.{SetOp, SetOpType, SetResult}
    import SetOpType._

    def bound: Tolerance
    override def meta = Metadata(Some(bound))

    // Only used on the ReservationServer instance
    object server {
      object m {
        val rpcs = metrics.create.counter("rpcs")
        val size_ops = metrics.create.counter("size_ops")
        val size_cached = metrics.create.counter("size_cached")
      }

      def execAdd(v: V)(c: CLevel)(k: UUID) =
        prepared.add(k, v)(c).execAsTwitter()

      def readSize(c: CLevel)(k: UUID) =
        prepared.size(k)(c).execAsTwitter()

      lazy val pool = new ReservationPool(name, bound, readSize)

      def getValue(op: SetOp) = {
        val v = op.value.get match {
          case Id(id) =>
            id.toUUID
          case e =>
            throw ReservationException(s"Unhandled Primitive type: $e")
        }
        TwTry {
          v.asInstanceOf[V]
        } rescue {
          case e: ClassCastException =>
            Throw(ReservationException(s"Wrong type for value: ${e.getMessage}"))
        }
      }

      def handle(op: SetOp): TwFuture[SetResult] = {
        m.rpcs += 1
        lazy val key = op.key.get.toUUID
        lazy val r = pool.get(key)

        op.op match {
          case Add =>
            getValue(op) map { v =>
              r.execute_if_immediate(1L, execAdd(v)) flatMap { early =>
                if (early) TwFuture(SetResult())
                else {
                  r submit {
                    r.execute(1L, execAdd(v))
                        .map(_ => SetResult())
                  }
                }
              }
            } get()

          case Remove =>
            getValue(op) map { v =>
              // TODO: requires another reservation pool
              prepared.remove(key, v)(CLevel.LOCAL_ONE).execAsTwitter()
                    .map(_ => SetResult())
            } get()

          case Contains =>
            getValue(op) map { v =>
              prepared.contains(key, v)(CLevel.LOCAL_ONE).execAsTwitter()
                    .map(r => SetResult(contains = Some(r)))
            } get()

          case Size =>
            m.size_ops += 1

            {
              for {
              // check again if expired because we may now be able to go
                updated <- r.update_if_expired(CLevel.LOCAL_ONE)
              } yield {
                if (!updated) m.size_cached += 1
                r.interval
              }
            } map { r =>
              SetResult(size = Some(r))
            }

          case Truncate =>
            Console.err.println(s"# Truncated $table")
            pool.clear()
            TwFuture(SetResult())

          case EnumUnknownSetOpType(e) =>
            throw ReservationException(s"Unknown op type: $e")
        }
      }
    }

    override def create(): Future[Unit] = {
      server.pool.init()
      createTwitter().asScala
    }

    override def truncate(): Future[Unit] = {
      tbl.truncate().execute().flatMap { _ =>
        reservations.clients.values
            .map(_.ipaSet(table, SetOp(Truncate)))
            .bundle()
      }.asScala.unit
    }

    type IPAType[T] = Inconsistent[T]
    type SizeType[T] = Interval[T]

    def arg(value: Option[V]) = value map {
      case v: UUID => ipa.thrift.Primitive.Id(value.get.toString)
      case _ => sys.error(s"Unsupported value type: $value")
    }

    // uses plain average latency to pick the server, tends to overload one (which is good for caching)
    def reservationOp(op: SetOpType, key: K, value: Option[V] = None) = {
      val (addr, tracker) = reservations.get
      val ts = tracker.start()
      reservations.clients(addr)
          .ipaSet(table, SetOp(op, key = Some(key.toString), value = arg(value)))
          .asScala
          .map { v => tracker.end(ts); v }
    }

    def reservationOpBalance(op: SetOpType, key: K, value: Option[V] = None) = {
      reservations.client
          .ipaSet(table, SetOp(op, key = Some(key.toString), value = arg(value)))
          .asScala
    }

    override def add(key: K, value: V): Future[Unit] =
      reservationOpBalance(Add, key, Some(value)).unit

    override def remove(key: K, value: V): Future[Unit] =
      reservationOpBalance(Remove, key, Some(value)).unit

    override def contains(key: K, value: V): Future[IPAType[Boolean]] =
      reservationOp(Contains, key, Some(value))
          .map { r => r.contains.get }

    override def size(key: K): Future[Interval[Long]] =
      reservationOp(Size, key)
        .map { r => r.size.get }
  }

  def fromNameAndBound[V:Primitive](name: String, bound: Bound)(implicit imps: CommonImplicits): IPASet[V] with Ops[V] = bound match {
    case Latency(l) =>
      new IPASet[V](name) with LatencyBound[V] { override val bound = l }

    case Consistency(Weak, Weak) =>
      new IPASet[V](name) with WeakOps[V]

    case Consistency(Weak, Strong) =>
      new IPASet[V](name) with WeakOps[V]

    case Consistency(Strong, _) =>
      new IPASet[V](name) with StrongOps[V]

    case t @ Tolerance(_) =>
      new IPASet[V](name) with ErrorBound[V] { override val bound = t }

    case e =>
      Console.err.println(s"Error creating BoundedCounter from bound: $e")
      sys.error(s"impossible case: $e")
  }

  def fromName[V:Primitive](name: String)(implicit imps: CommonImplicits): Try[IPASet[V] with Ops[V]] = {
    DataType.fromName(name, fromNameAndBound[V])
  }
}

abstract class IPASet[V:Primitive](val name: String)(implicit val imps: CommonImplicits) extends DataType(imps) {
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
