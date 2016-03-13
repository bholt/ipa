package ipa

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Function

import com.datastax.driver.core.{ConsistencyLevel => CLevel}
import com.twitter.concurrent.AsyncQueue
import com.twitter.util.{Future => TwFuture, Promise => TwPromise}
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.dsl._
import ipa.{thrift => th}
import org.apache.commons.lang.NotImplementedException
import owl._
import owl.Connector.config
import owl.Consistency._
import owl.Util._

import scala.collection.JavaConversions._
import scala.collection.immutable.IndexedSeq
import scala.collection.mutable
import scala.util.{Failure, Success, Try}


object BoundedCounter {

  case class DecrementException(value: Int)
      extends RuntimeException(s"Unable to decrement, already at minimum ($value).")

  def pack(i: Int, j: Int) = (i.toLong << 32) | (j & 0xffffffffL)
  def unpack(ij: Long) = ((ij >> 32).toInt, ij.toInt)

  trait Bounds {
    def cbound: Consistency
    type IPAType[T] <: Inconsistent[T]

    def valueResult(r: th.CounterResult): IPAType[Int]
    def decrResult(r: th.CounterResult): IPAType[Boolean]
  }

  trait StrongBounds extends Bounds { self: BoundedCounter =>
    override val cbound = Consistency(CLevel.QUORUM, CLevel.QUORUM)
    override def meta = Metadata(Some(cbound))
    type IPAType[T] = Consistent[T]
    def valueResult(r: th.CounterResult) = Consistent(r.value.get.toInt)
    def decrResult(r: th.CounterResult) = Consistent(r.success.get)
  }

  trait WeakBounds extends Bounds { self: BoundedCounter =>
    override val cbound = Consistency(CLevel.ONE, CLevel.ONE)
    override def meta = Metadata(Some(cbound))
    type IPAType[T] = Inconsistent[T]
    def valueResult(r: th.CounterResult) = Inconsistent(r.value.get.toInt)
    def decrResult(r: th.CounterResult) = Inconsistent(r.success.get)
  }

  trait ErrorBound extends Bounds { self: BoundedCounter =>
    def bound: Tolerance
    override val cbound = Consistency(CLevel.ONE, CLevel.ONE)
    override def meta = Metadata(Some(bound))
    type IPAType[T] = Interval[T]
    def valueResult(r: th.CounterResult) = ???
    def decrResult(r: th.CounterResult) = ???
  }

  def fromNameAndBound(name: String, bound: Bound)(implicit imps: CommonImplicits): BoundedCounter with Bounds = bound match {
    case Latency(l) =>
      throw new NotImplementedException("Latency bounds not implemented for BoundedCounter")

    case Consistency(Weak, Weak) =>
      new BoundedCounter(name) with WeakBounds

    case Consistency(Weak, Strong) =>
      new BoundedCounter(name) with WeakBounds

    case Consistency(Strong, _) =>
      new BoundedCounter(name) with StrongBounds

    case t @ Tolerance(_) =>
      new BoundedCounter(name) with ErrorBound { override val bound = t }

    case e =>
      Console.err.println(s"Error creating BoundedCounter from bound: $e")
      sys.error(s"impossible case: $e")
  }

  def fromName(name: String)(implicit imps: CommonImplicits): Try[BoundedCounter with Bounds] = {
    DataType.lookupMetadata(name) flatMap { metaStr =>
      val meta = Metadata.fromString(metaStr)
      meta.bound match {
        case Some(bound) =>
          Success(BoundedCounter.fromNameAndBound(name, bound))
        case _ =>
          Failure(th.ReservationException(s"Unable to find metadata for $name"))
      }
    } recoverWith {
      case e: Throwable =>
        Failure(th.ReservationException(s"metadata not found for $name"))
    }
  }

  def apply[T <: Bounds](name: String, bound: Bound = null)(implicit imps: CommonImplicits): BoundedCounter with T = {
    Option(bound) match {
      case Some(b) => fromNameAndBound(name, bound).asInstanceOf[BoundedCounter with T]
      case None => fromName(name).get.asInstanceOf[BoundedCounter with T]
    }
  }
}

class BoundedCounter(val name: String)(implicit val imps: CommonImplicits) extends DataType(imps) { self: BoundedCounter.Bounds =>
  import BoundedCounter._

  object m {
    lazy val balances = metrics.create.counter("balance")
    lazy val balance_retries = metrics.create.counter("balance_retry")
    lazy val consume_others = metrics.create.counter("consume_other")
    val forwards = metrics.create.counter("forwards")

    val transfers = metrics.create.counter("transfer")
    val transfers_failed = metrics.create.counter("transfer_failure")

    val inits = metrics.create.counter("init")
    val incrs = metrics.create.counter("incr")
    val decrs = metrics.create.counter("decr")
    val reads = metrics.create.counter("read")

    val cached = metrics.create.counter("cached")
    val expired = metrics.create.counter("expired")

    val consume_latency = metrics.create.timer("consume_latency")
    lazy val consume_other_latency = metrics.create.timer("consume_other_latency")
  }

  def replicas: IndexedSeq[Int] =
    session.getCluster.getMetadata.getAllHosts.map(_.getAddress.hashCode).toIndexedSeq

  val me: Int = this_host_hash

  class State(val key: UUID, var min: Int = 0, var version: Int = 0) extends FutureSerializer[th.CounterResult] {
    var lastReadAt = 0L
    var rights = new mutable.HashMap[(Int, Int), Int].withDefaultValue(0)
    var consumed = new mutable.HashMap[Int, Int].withDefaultValue(0)

    var tail: Option[TwFuture[th.CounterResult]] = None

    def rightsPacked = rights map { case ((i,j),v) => pack(i, j) -> v } toMap

    override def toString = s"($key -> min: $min, rights: $rights, consumed: $consumed)"

    def expired: Boolean = (System.nanoTime - lastReadAt) > config.lease.periodNanos

    def value: Int =
      min - consumed.values.sum +
          rights.map{
            case ((i,j),v) if i == j => v
            case _ => 0
          }.sum

    def localRights(who: Int = me): Int = {
      rights((who,who)) - consumed(who) +
          rights.map{
            case ((i,j),v) if i != who && j == who => v
            case ((i,j),v) if i == who && j != who => -v
            case _ => 0
          }.sum
    }

    def update_if_expired(): TwFuture[State] = {
      if (expired) {
        m.expired += 1
        update() map { _ => this }
      } else {
        m.cached += 1
        TwFuture { this }
      }
    }

    def init(min: Int): TwFuture[Unit] = {
      Console.err.println(s"##----- init $key $min -------------------")
      m.inits += 1
      this.min = min
      this.rights.clear()
      this.consumed.clear()
      prepared.init(key, min)(CLevel.ALL).execAsTwitter()
    }


    def update(): TwFuture[Unit] = {
      val time = System.nanoTime
      prepared.get(key)(cbound.read).execAsTwitter() map {
        case Some(st) =>
          lastReadAt = time
          min = st.min
          rights = st.rights
          consumed = st.consumed
        case _ =>
          sys.error(s"Unable to get State($key) on $this_host")
      } unit
    }

    def incr(n: Int = 1): TwFuture[Unit] = {
      m.incrs += 1
      val v = rights((me, me)) + n
      rights((me, me)) = v
      prepared.set(key, me, me, v)(cbound.write).execAsTwitter() onSuccess { _ =>
        // in the background, see if we should re-balance
        var myr = localRights(me)
        for (i <- replicas; r = localRights(i); if myr > 2*r && myr/3 > 0) yield {
          val t = myr/3
          myr -= t
          // Console.err.println(s"## incr:transfer $key => $t $me->$i")
          submit { transfer(t, i).map(_ => th.CounterResult()) }
        }
      }
    }

    def sufficient(n: Int): Boolean = value - n >= min

    def decr(n: Int = 1, retrying: Boolean = false): TwFuture[Boolean] = {
      if (!retrying) m.decrs += 1

      if (localRights() >= n) {
        Console.err.println(s"## decr: local $this")
        val v = consumed(me) + n
        consumed(me) = v
        prepared.consume(key, me, v)(cbound.write).execAsTwitter()
            .instrument(m.consume_latency)
            .map { _ => true }
      } else if (sufficient(n)) {
        Console.err.println(s"## decr: attempting to forward $this")
        // find replicas with rights available
        val reps = for {i <- replicas if localRights(i) >= n} yield i
        if (reps.isEmpty) {
          TwFuture(false)
        } else {
          m.forwards += 1
          val who = addrFromInt(reps.sample)
          // Console.err.println(s"### trying $who ($this)")
          // val cl = reservations.clients(who)
          // new Handle(key, cl).decr(n).map(_.get)
          TwFuture.exception(th.ForwardTo(who.getHostAddress))
        }
      } else if (cbound.write == Strong && !retrying) {
        Console.err.println(s"## decr: retrying $this")
        // if this is supposed to be Strong consistency,
        // then we have to try bypassing the cache to ensure we find any available
        update() flatMap { _ => decr(n, retrying = true) }
      } else {
        Console.err.println(s"## decr: aborting $this")
        TwFuture(false)
      }
    }

    def transfer(n: Int, to: Int): TwFuture[Unit] = {
      if (localRights() >= n) {
        m.transfers += 1
        val prev = rights(me, to)
        val v = prev + n
        prepared.transfer(key, (me, to), v, prev)(cbound.write)
            .execAsTwitter() onSuccess { _ =>
          rights += ((me, to) -> v)
          // Console.err.println(s"## transferred $n, $me -> $to: $this")
        } onFailure {
          case e =>
            Console.err.println(s"## error with transfer: ${e.getMessage}")
        }
      } else {
        m.transfers_failed += 1
        TwFuture.Unit
      }
    }

    def balance(promise: TwPromise[Unit] = null): TwPromise[Unit] = {
      val pr = if (promise != null) promise else TwPromise[Unit]()

      // Console.err.println(s"## balancing $this")

      val flatRights = replicas map { localRights(_) }
      val total = flatRights.sum
      assert(total == value, s"balance: total($total) != value($value)")
      val n = flatRights.size

      val balanced =
        replicas.zipWithIndex map { case (h, i) =>
          val (each, remain) = (total / n, total % n)
          (h,h) -> (each + (if (i < remain) 1 else 0))
        } toMap

      consumed.clear()
      rights.clear()
      consumed ++= replicas.map(_ -> 0).toMap
      rights ++= balanced
      version += 1

      prepared.balance(this)(cbound.write).execAsTwitter() onSuccess { succeeded =>
        if (succeeded) {
          m.balances += 1
          pr.setDone()
        } else {
          // retry
          m.balance_retries += 1
          update() flatMap { _ => balance(pr) }
        }
      }

      pr
    }
  }

  object State {

    def unpackMap(m: Map[Long, Int]) = m map { case (ij, v) => unpack(ij) -> v }

    def apply(key: UUID, min: Int, r: Map[Long,Int], c: Map[Int,Int]) = {
      val s = new State(key, min)
      s.rights ++= unpackMap(r)
      s.consumed ++= c
      s
    }
  }

  class StateTable extends CassandraTable[StateTable, State] {

    object key extends UUIDColumn(this) with PartitionKey[UUID]
    object min extends IntColumn(this)
    object version extends IntColumn(this)
    object rights extends MapColumn[StateTable, State, Long, Int](this)
    object consumed extends MapColumn[StateTable, State, Int, Int](this)

    override val tableName = name
    override def fromRow(r: Row) = State(key(r), min(r), rights(r), consumed(r))
  }

  val states = new StateTable

  override def create() = DataType.createWithMetadata(name, states, meta.toString).asScala
  override def truncate() = states.truncate().future().unit

  object prepared {
    private val (t, k, r, c) = (s"${space.name}.${states.tableName}", states.key.name, states.rights.name, states.consumed.name)
    private val (min, version) = (states.min.name, states.version.name)

    lazy val init: (UUID, Int) => (CLevel) => BoundOp[Unit] = {
      // initialize with replicas
      val rights = replicas map { i => pack(i,i) -> 0 } toMap
      val consumed = replicas map { i => i -> 0 } toMap
      val ps = session.prepare(
        s"UPDATE $t SET $min=?, $r=?, $c=?, $version=0 WHERE $k=?")
      (key: UUID, min: Int) => ps.bindWith(min, rights, consumed, key)(_ => ())
    }

    lazy val get: (UUID) => (CLevel) => BoundOp[Option[State]] = {
      val ps = session.prepare(s"SELECT * FROM $t WHERE $k = ?")
      (key: UUID) =>
        ps.bindWith(key)(_.first.map(states.fromRow))
    }

    lazy val set: (UUID, Int, Int, Int) => (CLevel) => BoundOp[Unit] = {
      val ps = session.prepare(s"UPDATE $t SET $r=$r+? WHERE $k = ?")
      (key: UUID, i: Int, j: Int, v: Int) => {
        ps.bindWith(Map(pack(i,j) -> v), key)(_ => ())
      }
    }

    /** get the outcome of a conditional update */
    private def condOutcome(rs: ResultSet) = {
      rs.first.exists(_.get(0, classOf[Boolean]))
    }

    lazy val transfer: (UUID, (Int, Int), Int, Int) => (CLevel) => BoundOp[Unit] = {
      val ps = session.prepare(s"UPDATE $t SET $r=$r+? WHERE $k = ?")
      (key: UUID, ij: (Int, Int), v: Int, prev: Int) => {
        val pij = pack(ij._1, ij._2)
        ps.bindWith(Map(pij -> v), key)(_ => ())
      }
    }

    lazy val balance: (State) => (CLevel) => BoundOp[Boolean] = {
      val ps = session.prepare(s"UPDATE $t SET $r=?, $c=?, $version=? WHERE $k=? IF $version=?")
      (st: State) => {
        ps.bindWith(st.rightsPacked, st.consumed, st.version, st.key, st.version-1)(condOutcome)
      }
    }

    lazy val consume: (UUID, Int, Int) => (CLevel) => BoundOp[Unit] = {
      val ps = session.prepare(s"UPDATE $t SET $c=$c+? WHERE $k=?")
      (key: UUID, i: Int, v: Int) => ps.bindWith(Map(i -> v), key)(_ => ())
    }

    /** consume from another replica, must do with conditional to force serial execution and ensure no one else beat us to it */
    lazy val consume_other: (UUID, Int, Int, Int, Int) => (CLevel) => BoundOp[(Boolean,Option[Map[Int,Int]], Option[Int])] = {
      val ps = session.prepare(s"UPDATE $t SET $c=$c+? WHERE $k=? IF $c[?] = ? AND $version=?")
      (key: UUID, who: Int, prev: Int, newv: Int, version: Int) =>
        ps.bindWith(Map(who -> newv), key, who, prev, version) { rs =>
          val r = rs.first.get
          (r.outcome, states.consumed.optional(r).toOption, states.version.optional(r).toOption)
        }
    }

  }

  val localStates = new ConcurrentHashMap[UUID, State]

  def get(key: UUID): TwFuture[Option[State]] =
    prepared.get(key)(cbound.read).execAsTwitter()

  def state(key: UUID): State = {
    localStates.computeIfAbsent(key, new Function[UUID,State] {
      override def apply(key: UUID) = new State(key)
    })
  }

  def local(key: UUID): TwFuture[State] = {
    val st = state(key)

    if (st.expired) {
      st.update().map(_ => st)
    } else {
      TwFuture.value(st)
    }
  }

  def handle(op: th.BoundedCounterOp): TwFuture[th.CounterResult] = {
    import th.CounterOpType._
    val key = op.key.toUUID

    val s = state(key)

    op.op match {

      case Init =>
        Console.err.println(s"## <- submit init($key)")
        s submit {
          s.init(op.n.get.toInt) map { _ => th.CounterResult() }
        }

      case Incr =>
        Console.err.println(s"## <- submit incr($key)")
        s submit {
          for {
            _ <- s.update_if_expired()
            _ <- s.incr(op.n.get.toInt)
          } yield {
            th.CounterResult()
          }
        }

      case Decr =>
        Console.err.println(s"## <- submit decr($key)")
        s submit {
          for {
            _ <- s.update_if_expired()
            success <- s.decr(op.n.get.toInt)
          } yield {
            th.CounterResult(success = Some(success))
          }
        }

      case Value =>
        Console.err.println(s"## <- submit value($key)")
        s submit {
          for {
            _ <- s.update_if_expired()
          } yield {
            m.reads += 1
            th.CounterResult(value = Some(s.value))
          }
        }

      case EnumUnknownCounterOpType(e) =>
        throw th.ReservationException(s"Unknown op type: $e")
    }
  }

  class Handle(val key: UUID, client: ReservationService = reservations.client) {

    def table = th.Table(space.name, name)

    import ipa.thrift.CounterOpType._
    import th.{BoundedCounterOp => Op}

    def init(min: Int = 0): TwFuture[Unit] =
      client.boundedCounter(table, Op(Init, key.toString, Some(min))).unit

    def incr(by: Int = 1): TwFuture[Unit] =
      client.boundedCounter(table, Op(Incr, key.toString, Some(by))).unit

    def decr(by: Int = 1): TwFuture[IPAType[Boolean]] = {
      client.boundedCounter(table, Op(Decr, key.toString, Some(by)))
          .map(decrResult)
          .rescue {
            case th.ForwardTo(who) =>
              new Handle(key, reservations.clients(InetAddress.getByName(who))).decr(by)
          }
    }

    def value(): TwFuture[IPAType[Int]] =
      client.boundedCounter(table, Op(Value, key.toString)).map(valueResult)
  }

  def apply(key: UUID) = new Handle(key)

}
