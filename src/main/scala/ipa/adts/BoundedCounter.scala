package ipa.adts

import java.net.InetAddress
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Function

import com.datastax.driver.core.exceptions.WriteTimeoutException
import com.datastax.driver.core.{ConsistencyLevel => CLevel}
import com.twitter.util.{Duration => TwDuration, Future => TwFuture}
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.dsl._
import ipa.Connector.config
import ipa.Util._
import ipa.thrift.ReservationException
import ipa.types.Consistency._
import ipa.types._
import ipa.{thrift => th, _}

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
    type IPAValueType[T] <: Inconsistent[T]
    type IPADecrType[T] <: Inconsistent[T]

    def ebound: Option[Tolerance] = None
    def lbound: Option[Latency] = None

    def valueResult(r: th.CounterResult): IPAValueType[Int]
    def decrResult(r: th.CounterResult): IPADecrType[Boolean]
  }

  trait StrongBounds extends Bounds { self: BoundedCounter =>
    override val cbound = Consistency(CLevel.QUORUM, CLevel.QUORUM)
    override def meta = Metadata(Some(cbound))
    type IPAValueType[T] = Consistent[T]
    type IPADecrType[T] = Consistent[T]
    def valueResult(r: th.CounterResult) = Consistent(r.value.get.toInt)
    def decrResult(r: th.CounterResult) = Consistent(r.success.get)
  }

  trait WeakBounds extends Bounds { self: BoundedCounter =>
    override val cbound = Consistency(CLevel.LOCAL_ONE, CLevel.LOCAL_ONE)
    override def meta = Metadata(Some(cbound))
    type IPAValueType[T] = Inconsistent[T]
    type IPADecrType[T] = Inconsistent[T]
    def valueResult(r: th.CounterResult) = Inconsistent(r.value.get.toInt)
    def decrResult(r: th.CounterResult) = Inconsistent(r.success.get)
  }

  trait ErrorBound extends Bounds { self: BoundedCounter =>
    def bound: Tolerance
    override val cbound = Consistency(CLevel.LOCAL_ONE, CLevel.LOCAL_ONE)
    override def ebound = Some(bound)

    override def meta = Metadata(Some(bound))

    type IPAValueType[T] = Interval[T]
    type IPADecrType[T] = Inconsistent[T]

    def valueResult(r: th.CounterResult) = Interval(r.min.get, r.max.get)
    def decrResult(r: th.CounterResult) = Inconsistent(r.success.get)
  }

  trait LatencyBound extends Bounds { self: BoundedCounter =>
    def bound: Latency
    override val cbound = Consistency(CLevel.LOCAL_ONE, CLevel.LOCAL_ONE)
    override def lbound = Some(bound)

    override def meta = Metadata(Some(bound))

    type IPAValueType[T] = Rushed[T]
    type IPADecrType[T] = Rushed[T]

    def valueResult(r: th.CounterResult) =
      Rushed(r.value.get.toInt, CLevel.valueOf(r.consistency.get))
    def decrResult(r: th.CounterResult) =
      Rushed(r.success.get, CLevel.valueOf(r.consistency.get))
  }

  def fromNameAndBound(name: String, bound: Bound)(implicit imps: CommonImplicits): BoundedCounter with Bounds = bound match {
    case l @ Latency(_) =>
      new BoundedCounter(name) with LatencyBound { override val bound = l }

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
        Failure(th.ReservationException(s"metadata not found for ${imps.space.name}.$name"))
    }
  }

  def apply[T <: Bounds](name: String, bound: Bound = null)(implicit imps: CommonImplicits): BoundedCounter with T = {
    Option(bound) match {
      case Some(b) => fromNameAndBound(name, bound).asInstanceOf[BoundedCounter with T]
      case None => fromName(name).get.asInstanceOf[BoundedCounter with T]
    }
  }
}

class BoundedCounter(val name: String)(implicit val imps: CommonImplicits) extends DataType(imps) with RushCommon { self: BoundedCounter.Bounds =>
  import BoundedCounter._

  object m {
    lazy val balances = metrics.create.counter("balance")
    lazy val balance_retries = metrics.create.counter("balance_retry")
    lazy val consume_others = metrics.create.counter("consume_other")
    val forwards = metrics.create.counter("forwards")
    val forwarded_again = metrics.create.counter("forwarded_again")
    lazy val init_retries = metrics.create.counter("init_retry")
    lazy val sync_retries = metrics.create.counter("sync_retry")

    lazy val travel_time = metrics.create.histogram("travel_time")

    val transfers = metrics.create.counter("transfer")
    val transfer_reactive = metrics.create.counter("transfer_reactive")
    val transfers_failed = metrics.create.counter("transfer_failure")
    val transferred_total = metrics.create.counter("transferred_total")

    val inits = metrics.create.counter("init")
    val incrs = metrics.create.counter("incr")
    val decrs = metrics.create.counter("decr")
    val reads = metrics.create.counter("read")

    val sync_proactive = metrics.create.counter("sync_proactive")
    val sync_skipped = metrics.create.counter("sync_skipped")
    val sync_blocking = metrics.create.counter("sync_blocking")
    val cached = metrics.create.counter("cached")
    val expired = metrics.create.counter("expired")

    val consume_latency = metrics.create.timer("consume_latency")
    val get_latency = metrics.create.timer("get_latency")
    lazy val sync_latency = metrics.create.timer("sync_latency")
    lazy val consume_other_latency = metrics.create.timer("consume_other_latency")
    lazy val transfer_latency = metrics.create.timer("transfer_latency")
  }

  def replicas: IndexedSeq[Int] =
    session.getCluster.getMetadata.getAllHosts.map(_.getAddress.hashCode).toIndexedSeq

  val me: Int = this_host_hash

  class State(val key: UUID, var min: Int = 0, var version: Int = 0) extends FutureSerializer {
    var lastReadAt = 0L
    var rights = new mutable.HashMap[(Int, Int), Int].withDefaultValue(0)
    var consumed = new mutable.HashMap[Int, Int].withDefaultValue(0)

    val pendingTransfers = new mutable.HashMap[Int, Int]()

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

    def interval: Interval[Int] = {
      ebound match {
        case Some(t) =>
          val v = value
          val d = t.delta(v)
          Interval(v-d, v+d)
        case None =>
          throw th.ReservationException("Called interval without an error bound.")
      }
    }

    def localRights(who: Int = me): Int = {
      rights((who,who)) - consumed(who) +
          rights.map{
            case ((i,j),v) if i != who && j == who => v
            case ((i,j),v) if i == who && j != who => -v
            case _ => 0
          }.sum
    }

    def update_if_expired(): TwFuture[State] = {
      if (expired || cbound.isStrong) {
        m.expired += 1
        update() map { _ => this }
      } else {
        m.cached += 1
        TwFuture { this }
      }
    }

    def init(min: Int): TwFuture[Unit] = {
      m.inits += 1
      this.min = min
      this.rights.clear()
      this.consumed.clear()
      prepared.init(key, min)(CLevel.ALL).execAsTwitter().rescue {
        case e: WriteTimeoutException =>
          Console.err.println(s"!! WriteTimeoutException in BoundedCounter.init")
          e.printStackTrace()
          init(min)
        case e: Throwable =>
          Console.err.println(s"!! exception in BoundedCounter.init: ${e.getMessage}")
          e.printStackTrace()
          TwFuture.exception(e)
      }
    }

    def update(c: CLevel = cbound.read): TwFuture[Unit] = {
      val time = System.nanoTime
      prepared.get(key)(c).execAsTwitter().instrument(m.get_latency) map {
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
        val myr = localRights(me)
        val rs = replicas
        if (myr / rs.size > 0) {
          // transfers: only those with significantly fewer rights
          val ts = for (i <- rs; r = localRights(i); if myr > 2 * r) yield (i, r)

          val total = myr + ts.map(_._2).sum
          // compute how much each should have if it was evenly distributed
          val each = total / (ts.size+1)
          for ((who, v) <- ts) {
            // transfer the difference to get it up to the even distribution
            val n = each - v
            // transfer 'n' to 'i' in the background
            pendingTransfers(who) = n
            submit { transfer() }
          }
        }
      }
    }

    def sufficient(n: Int): Boolean = value - n >= min

    def sync(): TwFuture[Unit] = {
      rights((me,me)) -= consumed(me)
      consumed(me) = 0
      prepared.sync(key, me, rights((me,me)), consumed(me))(CLevel.ALL)
          .execAsTwitter().instrument(m.sync_latency)
          .rescue {
            case e: WriteTimeoutException =>
              m.sync_retries += 1
              Console.err.println(s"!! WriteTimeoutException in BoundedCounter.sync")
              sync()
          }
    }

    def should_sync_soon: Boolean =
      ebound.isDefined && consumed(me) >= (maxConsumable * 2 / 3)

    def sync_if_needed(n: Int): TwFuture[Unit] = {
      if (ebound.isDefined && consumed(me) + n >= maxConsumable) {
        m.sync_blocking += 1
        sync()
      } else {
        TwFuture.Unit
      }
    }

    def maxConsumable = ebound map { t =>
      t.delta(value) / session.nreplicas
    } getOrElse {
      Int.MaxValue
    }

    def localRush[T](estTravelTime: Option[Long], cdefault: CLevel)(exec: CLevel => TwFuture[T]): TwFuture[Inconsistent[T]] = {
      lbound match {
        case Some(Latency(total)) =>
          val ett = TwDuration.fromNanoseconds(estTravelTime.get)
          val remain = total - (ett * 2)
          rush(remain) { exec }
        case None =>
          exec(cdefault).map(Inconsistent(_))
      }
    }

    def decr(n: Int = 1, retrying: Boolean = false,
        clevel: CLevel = cbound.write,
        forwarded: Boolean = false): TwFuture[Boolean] =
    {
      if (!retrying) m.decrs += 1

      if (localRights() >= n) {
        val v = consumed(me) + n
        consumed(me) = v
        prepared.consume(key, me, v)(clevel)
          .execAsTwitter().map(_ => true)
          .instrument(m.consume_latency)

      } else if (sufficient(n)) {
        // find replicas with rights available
        val reps = for {i <- replicas if localRights(i) >= n} yield i
        if (reps.isEmpty) {
          TwFuture(false)
        } else {
          if (forwarded) m.forwarded_again += 1
          m.forwards += 1
          val who = addrFromInt(reps.sample)
          TwFuture.exception(th.ForwardTo(who.getHostAddress))
        }
      } else if (lbound.isEmpty && cbound.write == Strong && !retrying) {
        // if this is supposed to be Strong consistency,
        // then we have to try bypassing the cache to ensure we find any available
        update(clevel) flatMap { _ => decr(n, retrying = true, clevel = clevel) }
      } else {
        TwFuture(false)
      }
    }

    def transfer(): TwFuture[Unit] = {
      lazy val lr = localRights()
      val ts: Map[Int,Int] = {
        val tmp = pendingTransfers.filter(_._2 <= lr)
        if (tmp.values.sum <= lr) tmp.toMap
        else if (tmp.nonEmpty) Map(tmp.maxBy(_._2)) // otherwise pick the biggest
        else Map()
      }
      m.transfers_failed += (pendingTransfers.size - ts.size)
      pendingTransfers.clear()
      m.transferred_total += ts.values.sum
      if (ts.isEmpty) {
        m.transfers_failed += 1
        TwFuture.Unit
      } else {
        val newv = ts map { case (to, inc) => to -> (rights((me,to)) + inc) }
        prepared.transfer(key, newv)(cbound.write)
            .execAsTwitter().instrument(m.transfer_latency) onSuccess { _ =>
          rights ++= newv map { case (to, n) => (me, to) -> n }
        } onFailure {
          case e =>
            Console.err.println(s"## error with transfer: ${e.getMessage}")
        }
      }
    }

    def balance(): TwFuture[Unit] = {
      // try to find who we were forwarded from
      val mr = localRights(me)
      val ts = for (i <- replicas; r = localRights(i); if 2*r < mr) yield (i, r)
      if (ts.nonEmpty) {
        val who = ts.minBy(_._2)._1
        pendingTransfers(who) = mr / 2
        submit {
          m.transfer_reactive += 1
          transfer()
        }
      } else {
        TwFuture.Unit
      }
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
  override def truncate() = {
    states.truncate().future().unit flatMap { _ =>
      reservations.clients.values map { c =>
        c.boundedCounter(table, th.BoundedCounterOp(th.CounterOpType.Truncate))
      } bundle() asScala
    } unit
  }

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

    lazy val transfer: (UUID, Map[Int, Int]) => (CLevel) => BoundOp[Unit] = {
      val ps = session.prepare(s"UPDATE $t SET $r=$r+? WHERE $k = ?")
      (key: UUID, transfers: Map[Int,Int]) => {
        val packed = transfers map { case (to,v) => pack(me,to) -> v }
        ps.bindWith(packed, key)(_ => ())
      }
    }

    lazy val balance: (State) => (CLevel) => BoundOp[Boolean] = {
      val ps = session.prepare(s"UPDATE $t SET $r=?, $c=?, $version=? WHERE $k=? IF $version=?")
      (st: State) => {
        ps.bindWith(st.rightsPacked, st.consumed, st.version, st.key, st.version-1)(condOutcome)
      }
    }

    // cancel out rights/consumed to show that we've synchronized our changes
    lazy val sync: (UUID, Int, Int, Int) => (CLevel) => BoundOp[Unit] = {
      val ps = session.prepare(s"UPDATE $t SET $r=$r+?, $c=$c+? WHERE $k=?")
      (key: UUID, me: Int, newRights: Int, newConsumed: Int) =>
        ps.bindWith(Map(pack(me,me) -> newRights), Map(me -> newConsumed), key)(_=>())
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

    lazy val s = state(op.key.get.toUUID)

    op.op match {
      case Truncate =>
        Console.err.println(s"# Truncated $table")
        localStates.clear()
        TwFuture(th.CounterResult())

      case Init =>
        s submit {
          s.init(op.n.get.toInt) map { _ => th.CounterResult() }
        }

      case Incr =>
        s submit {
          for {
            _ <- s.update_if_expired()
            _ <- s.incr(op.n.get.toInt)
          } yield {
            th.CounterResult()
          }
        }

      case Decr =>
        s submit {
          val n = op.n.get.toInt
          for {
            _ <- s.update_if_expired()
            _ <- s.sync_if_needed(n)
            r <- s.localRush(op.estTravelTime, cbound.write) { c: CLevel =>
              s.decr(n, clevel = c, forwarded = op.forwarded)
            }
          } yield {

            if (op.forwarded) {
              s.balance()
            }

            if (s.should_sync_soon) {
              s submit {
                // double-check that we should still do sync
                // (may have been done already)
                if (s.should_sync_soon) {
                  m.sync_proactive += 1
                  s.sync().map(_ => th.CounterResult())
                } else {
                  m.sync_skipped += 1
                  TwFuture.Unit
                }
              }
            }
            r match {
              case r: Rushed[_] =>
                th.CounterResult(success = Some(r.get),
                    consistency = Some(r.consistency.name))
              case r: Inconsistent[_] =>
                th.CounterResult(success = Some(r.get))
            }
          }
        }

      case Value =>
        val arrival = System.nanoTime
        s.localRush(op.estTravelTime, cbound.read) {
          case Strong =>
            s submit { s.update(Strong) map { _ => m.reads += 1; s.value } }
          case CLevel.ONE | CLevel.LOCAL_ONE =>
            if (!s.expired) {
              TwFuture(s.value)
            } else {
              s submit {
                s.update_if_expired() map { _ =>
                  m.reads += 1
                  s.value
                }
              }
            }
          case c =>
            TwFuture.exception(ReservationException(
              s"Unsupported consistency level: $c"))
        } map { r =>
          val elapsed = System.nanoTime - arrival
          r match {
            case v: Rushed[_] =>
              th.CounterResult(
                value = Some(v.get),
                consistency = Some(v.consistency.name),
                processingTime = Some(elapsed)
              )
            case v: Inconsistent[_] =>
              if (ebound.isDefined) {
                val i = s.interval
                th.CounterResult(min = Some(i.min), max = Some(i.max))
              } else {
                th.CounterResult(value = Some(s.value))
              }
          }
        }

      case EnumUnknownCounterOpType(e) =>
        throw th.ReservationException(s"Unknown op type: $e")
    }
  }

  class Handle(val key: UUID, client: ReservationService = reservations.client) {

    import ipa.thrift.CounterOpType._
    import th.{BoundedCounterOp => Op}

    def init(min: Int = 0): TwFuture[Unit] =
      client.boundedCounter(table, Op(Init, Some(key.toString), Some(min))).unit

    def incr(by: Int = 1): TwFuture[Unit] =
      client.boundedCounter(table, Op(Incr, Some(key.toString), Some(by))).unit

    def decr(by: Int = 1, forwarded: Boolean = false): TwFuture[IPADecrType[Boolean]] = {
      client.boundedCounter(table,
        Op(Decr, Some(key.toString), Some(by), forwarded, estTravelTime))
          .map(decrResult)
          .rescue {
            case th.ForwardTo(who) =>
              new Handle(key, reservations.clients(InetAddress.getByName(who)))
                  .decr(by, forwarded = true)
          }
    }

    private def estTravelTime: Option[Long] = lbound map { _ =>
      m.travel_time.getSnapshot.getMean.toLong
    }

    def value(): TwFuture[IPAValueType[Int]] = {
      val startTime = System.nanoTime
      client.boundedCounter(table,
        Op(Value, Some(key.toString), estTravelTime = estTravelTime)
      ).map { r =>
        r.processingTime foreach { t =>
            val elapsed = System.nanoTime - startTime
          m.travel_time << (elapsed - t) / 2
        }
        r
      }.map(valueResult)
    }
  }

  def apply(key: UUID) = new Handle(key)

}
