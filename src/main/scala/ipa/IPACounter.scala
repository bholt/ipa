package ipa

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Function

import com.datastax.driver.core.{Row, ConsistencyLevel => CLevel}
import com.twitter.util.{Future => TwFuture}
import com.twitter.{util => tw}
import com.websudos.phantom.dsl.{UUID, _}
import com.websudos.phantom.keys.PartitionKey
import ipa.{thrift => th}
import ipa.thrift.ReservationException
import owl.Consistency.{apply => _, _}
import owl.Conversions._
import owl.Util._
import owl._

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds
import scala.util.{Failure, Success, Try}

object IPACounter {
  import Consistency._

  trait Ops {
    type IPAType[T] <: Inconsistent[T]
    def incr(key: UUID, by: Long): Future[Unit]
    def read(key: UUID): Future[IPAType[Long]]
  }

  trait WeakOps extends Ops { base: IPACounter =>
    type IPAType[T] = Inconsistent[T]
    override def read(key: UUID) =
      base.read(Weak)(key).map(Inconsistent(_))
    override def incr(key: UUID, by: Long) =
      base.incr(Strong)(key, by)
  }

  trait WeakWeakOps extends Ops { base: IPACounter =>
    type IPAType[T] = Inconsistent[T]
    override def read(key: UUID) =
      base.read(Weak)(key).map(Inconsistent(_))
    override def incr(key: UUID, by: Long) =
      base.incr(Weak)(key, by)
  }

  trait StrongOps extends Ops { base: IPACounter =>
    type IPAType[T] = Consistent[T]
    override def read(key: UUID): Future[Consistent[Long]] =
      base.read(Strong)(key) map { Consistent(_) }
    override def incr(key: UUID, by: Long): Future[Unit] =
      base.incr(Strong)(key, by)
  }

  trait LatencyBound extends Ops with RushImpl {
    base: IPACounter =>

    def bound: FiniteDuration

    type IPAType[T] = Rushed[T]

    override def read(key: UUID) =
      rush(bound){ c: CLevel => base.read(c)(key) }

    override def incr(key: UUID, by: Long) =
      base.incr(Strong)(key, by)
  }

  trait ErrorTolerance extends Ops {
    base: IPACounter =>

    def tolerance: Tolerance

    override def meta = Metadata(Some(tolerance))

    override def create(): Future[Unit] = {
      server.pool.init()
      createTwitter().asScala
    }

    override def truncate(): Future[Unit] = {
      tbl.truncate.future() flatMap { _ =>
        reservations.clients.values
            .map(_.counter(table, th.BoundedCounterOp(th.CounterOpType.Truncate)))
            .bundle().asScala.unit
      }
    }

    type IPAType[T] = Interval[T]

    import ipa.thrift.{BoundedCounterOp => Op}
    import ipa.thrift.CounterResult
    import ipa.thrift.CounterOpType._

    override def incr(key: UUID, by: Long): Future[Unit] = {
      reservations.client
          .counter(table, Op(Incr, Some(key.toString), Some(by)))
          .asScala.unit
    }

    override def read(key: UUID): Future[Interval[Long]] = {
      reservations.client
          .counter(table, Op(Value, Some(key.toString)))
          .map(r => Interval(r.min.get.toLong, r.max.get.toLong))
          .asScala
    }

    // Only used on the ReservationServer instance
    object server {

      object m {
        val rpcs = metrics.create.counter("rpcs")
        val reads = metrics.create.counter("reads")
        val incrs = metrics.create.counter("incrs")
        val cached_reads = metrics.create.counter("cached_reads")
      }

      def base_incr(n: Long)(c: CLevel)(key: UUID) = base.incrTwitter(c)(key, n)

      lazy val pool = new ReservationPool(name, tolerance, base.readTwitter)

      case class Pending(var incrs: AtomicInteger = new AtomicInteger)
      val pendingIncrMap = new ConcurrentHashMap[UUID, Pending]()
      def pending(key: UUID) =
        pendingIncrMap.computeIfAbsent(key, new Function[UUID,Pending]{
          def apply(key: UUID) = Pending()
        })

      def handle(op: Op): TwFuture[CounterResult] = {
        m.rpcs += 1
        lazy val key = op.key.get.toUUID
        lazy val r = pool.get(key)

        op.op match {

          case Incr =>
            val p = pending(key)
            p.incrs.addAndGet(op.n.get.toInt)
            m.incrs += 1
            r submit {

              val n = p.incrs.getAndSet(0)
              val f = if (n > 0) r.execute(n, base_incr(n))
                      else TwFuture.Unit
              f.map { _ => th.CounterResult() }
            }

          case Value =>
            {
              m.reads += 1
              if (r.lastRead.expired) {
                r submit {
                  for {
                    _ <- r.fetchAndUpdate(CLevel.LOCAL_ONE)
                  } yield {
                    r.interval
                  }
                }
              } else {
                m.cached_reads += 1
                TwFuture(r.interval)
              }
            } map { r =>
              CounterResult(min = Some(r.min.toInt), max = Some(r.max.toInt))
            }

          case Truncate =>
            Console.err.println(s"# Truncated $table")
            pool.clear()
            TwFuture(CounterResult())

          case Init | Decr =>
            throw ReservationException(s"Unsupported op type: ${op.op}")
          case EnumUnknownCounterOpType(e) =>
            throw ReservationException(s"Unknown op type: $e")
        }
      }
    }
  }

  def fromNameAndBound(name: String, bound: Bound)(implicit imps: CommonImplicits) = bound match {
    case Latency(l) =>
      new IPACounter(name) with IPACounter.LatencyBound { override val bound = l }

    case Consistency(Weak, Weak) =>
      new IPACounter(name) with IPACounter.WeakWeakOps

    case Consistency(Weak, Strong) =>
      new IPACounter(name) with IPACounter.WeakOps

    case Consistency(Strong, _) =>
      new IPACounter(name) with IPACounter.StrongOps

    case t @ Tolerance(_) =>
      new IPACounter(name) with IPACounter.ErrorTolerance { override val tolerance = t }

    case e =>
      Console.err.println("error handling bound")
      sys.error(s"impossible case: $e")
  }

  def fromName(name: String)(implicit imps: CommonImplicits): Try[IPACounter] = {
    DataType.lookupMetadata(name) flatMap { metaStr =>
      val meta = Metadata.fromString(metaStr)
      meta.bound match {
        case Some(bound) =>
          Success(IPACounter.fromNameAndBound(name, bound))
        case _ =>
          Failure(ReservationException(s"Unable to find metadata for $name"))
      }
    } recoverWith {
      case e: Throwable =>
        Failure(ReservationException(s"metadata not found for $name"))
    }
  }
}

class IPACounter(val name: String)(implicit val imps: CommonImplicits) extends DataType(imps) {
  self: IPACounter.Ops =>

  case class Count(key: UUID, count: Long)

  class CountTable extends CassandraTable[CountTable, Count] {
    object key extends UUIDColumn(this) with PartitionKey[UUID]
    object value extends CounterColumn(this)

    override val tableName = name
    override def fromRow(r: Row) = Count(key(r), value(r))
  }

  val tbl = new CountTable

  def createTwitter(): tw.Future[Unit] =
    DataType.createWithMetadata(name, tbl, meta.toString)

  override def create(): Future[Unit] =
    createTwitter().asScala

  override def truncate(): Future[Unit] =
    tbl.truncate.future().unit

  class Handle(val key: UUID) {
    def incr(by: Long = 1L): Future[Unit] = self.incr(key, by)
    def read(): Future[IPAType[Long]] = self.read(key)
  }
  def apply(key: UUID) = new Handle(key)

  object prepared {
    private val (k, v, t) = (tbl.key.name, tbl.value.name, s"${space.name}.$name")

    lazy val read: (UUID) => (CLevel) => BoundOp[Long] = {
      val ps = session.prepare(s"SELECT $v FROM $t WHERE $k = ?")
      key: UUID => ps.bindWith(key)(_.first.map(tbl.value(_)).getOrElse(0L))
    }

    lazy val incr: (UUID, Long) => (CLevel) => BoundOp[Unit] = {
      val ps = session.prepare(s"UPDATE $t SET $v = $v + ? WHERE $k = ?")
      (key: UUID, by: Long) => ps.bindWith(by, key)(_ => ())
    }
  }

  def read(c: CLevel)(key: UUID) =
    prepared.read(key)(c).execAsScala().instrument()

  def readTwitter(c: CLevel)(key: UUID): tw.Future[Long] = {
    prepared.read(key)(c).execAsTwitter().instrument()
  }

  def incr(c: CLevel)(key: UUID, by: Long) =
    prepared.incr(key,by)(c).execAsScala().instrument()

  def incrTwitter(c: CLevel)(key: UUID, by: Long): tw.Future[Unit] =
    prepared.incr(key, by)(c).execAsTwitter().instrument()

}
