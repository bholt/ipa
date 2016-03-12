package ipa

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Function

import com.datastax.driver.core.{ConsistencyLevel => CLevel}
import com.twitter.concurrent.AsyncQueue
import com.twitter.util.{Future => TwFuture, Promise => TwPromise}
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.dsl._
import ipa.thrift._
import owl.Connector.config
import owl.Util._

import scala.collection.JavaConversions._
import scala.collection.immutable.IndexedSeq
import scala.collection.mutable

class BoundedCounter(val name: String)(implicit val imps: CommonImplicits) extends DataType(imps) {
  import BoundedCounter._

  object m {
    val balances = metrics.create.counter("balance")
    val balance_retries = metrics.create.counter("balance_retry")
    val consume_others = metrics.create.counter("consume_other")
    val consume_other_attempts = metrics.create.counter("consume_other_attempt")
    val piggybacks = metrics.create.counter("piggybacks")

    val consume_latency = metrics.create.timer("consume_latency")
    val consume_other_latency = metrics.create.timer("consume_other_latency")
  }

  def replicas: IndexedSeq[Int] =
    session.getCluster.getMetadata.getAllHosts.map(_.getAddress.hashCode).toIndexedSeq

  val me: Int = this_host_hash

  class State(val key: UUID, var min: Int = 0, var version: Int = 0) {
    var updating: Option[TwPromise[Unit]] = None
    var lastReadAt = 0L
    val rights = new mutable.HashMap[(Int, Int), Int].withDefaultValue(0)
    val consumed = new mutable.HashMap[Int, Int].withDefaultValue(0)

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

    def update(): TwFuture[Unit] = this.synchronized {
      updating match {
        case Some(pr) =>
          m.piggybacks += 1
          pr
        case None =>
          val pr = TwPromise[Unit]()
          updating = Some(pr)
          val time = System.nanoTime
          prepared.get(key)(CLevel.QUORUM).execAsTwitter() onSuccess { opt =>
            opt match {
              case Some(st) =>
                lastReadAt = time
                min = st.min
                rights ++= st.rights
                consumed ++= st.consumed
              case _ =>
                sys.error(s"Unable to get State($key) on $this_host")
            }
            updating = None
            pr.setDone()
          }
          pr
      }
    }

    def incr(n: Int = 1): TwFuture[Unit] = {
      val v = rights((me, me)) + n
      rights += ((me, me) -> v)
      prepared.set(key, me, me, v)(CLevel.QUORUM).execAsTwitter()
    }

    def sufficient(n: Int): Boolean = value - n >= min

    def decr(n: Int = 1): TwFuture[Boolean] = {
      if (!sufficient(n)) return TwFuture.value(false)

      {
        if (localRights() < n) balance()
        else TwFuture.Unit
      } flatMap { _ =>
        Console.err.println(s"## balanced: $key -> ${rights.values.toList} ${consumed.values.toList}")
        if (localRights() >= n) {
          val v = consumed(me) + n
          consumed += (me -> v)
          prepared.consume(key, me, v)(CLevel.QUORUM).execAsTwitter()
              .instrument(m.consume_latency)
              .map { _ => true }
        } else if (sufficient(n)) {
          m.consume_others += 1
          var retries = 0
          retry { r: Boolean => r || !sufficient(n) || retries > 10} {
            Console.err.println(s"### consume_other: $consumed")
            retries += 1
            m.consume_other_attempts += 1
            // find replicas with rights available
            val reps = for {i <- replicas if localRights(i) >= n} yield i
            if (reps.isEmpty) {
              TwFuture.value(false)
            } else {
              val who = reps.sample
              Console.err.println(s"### trying $who ($this)")
              val c = consumed(who)
              prepared.consume_other(key, who, c, c + 1, version)(CLevel.QUORUM)
                .execAsTwitter().instrument(m.consume_other_latency)
                .flatMap {
                  case (true, _, _) =>
                    TwFuture.value(true)
                  case (false, nconsumed, Some(nversion)) =>
                    if (nversion != version) {
                      Console.err.println("#### concurrent re-balance!")
                      update().map(_ => false)
                    } else {
                      Console.err.println(s"### failed\n### tried: {$who: ${c+1}}\n### saw: $consumed\n- $this")
                      // update so we can try again
                      consumed ++= nconsumed
                      TwFuture.value(false)
                    }
                  case _ =>
                    sys.error("Incorrect values from consume_other")
                }
            }
          }
        } else {
          TwFuture.value(false)
        }
      }
    }

    def transfer(n: Int, to: Int, promise: TwPromise[Int] = null, retries: Int = 0): TwFuture[Int] = {
      require(localRights() >= n)
      val p = if (promise != null) promise else TwPromise[Int]()
      val prev = rights(me, to)
      val v = prev + n
      rights += ((me, to) -> v)
      prepared.transfer(key, (me, to), v, prev)(CLevel.QUORUM).execAsTwitter() onSuccess {
        success =>
          if (success) {
            promise.setValue(retries)
          } else {
            update() flatMap { _ => transfer(n, to, p, retries + 1) }
          }
      }
      p
    }

    def balance(promise: TwPromise[Unit] = null): TwPromise[Unit] = {
      val pr = if (promise != null) promise else TwPromise[Unit]()

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

      prepared.balance(this)(CLevel.QUORUM).execAsTwitter() onSuccess { succeeded =>
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

  override def create() = states.create.ifNotExists().future().unit
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

    lazy val transfer: (UUID, (Int, Int), Int, Int) => (CLevel) => BoundOp[Boolean] = {
      val ps = session.prepare(s"UPDATE $t SET $r=$r+? WHERE $k = ? IF $r[?] = ?")
      (key: UUID, ij: (Int, Int), v: Int, prev: Int) => {
        val pij = pack(ij._1, ij._2)
        ps.bindWith(Map(pij -> v), key, pij, prev)(condOutcome)
      }
    }

    lazy val balance: (State) => (CLevel) => BoundOp[Boolean] = {
      val ps = session.prepare(s"UPDATE $t SET $r=?, $c=?, $version=? WHERE $k=? IF $version=?")
      (st: State) => {
        ps.bindWith(st.rightsPacked, st.consumed, st.version, st.key, st.version-1) {
          _.first.exists(_.get(0, classOf[Boolean]))
        }
      }
    }

    lazy val consume: (UUID, Int, Int) => (CLevel) => BoundOp[Unit] = {
      val ps = session.prepare(s"UPDATE $t SET $c=$c+? WHERE $k=?")
      (key: UUID, i: Int, v: Int) => ps.bindWith(Map(i -> v), key)(_ => ())
    }

    /** consume from another replica, must do with conditional to force serial execution and ensure no one else beat us to it */
    lazy val consume_other: (UUID, Int, Int, Int, Int) => (CLevel) => BoundOp[(Boolean,Map[Int,Int], Option[Int])] = {
      val ps = session.prepare(s"UPDATE $t SET $c=$c+? WHERE $k=? IF $c[?] = ? AND $version=?")
      (key: UUID, who: Int, prev: Int, newv: Int, version: Int) =>
        ps.bindWith(Map(who -> newv), key, who, prev, version) { rs =>
          val r = rs.first.get
          (r.outcome, states.consumed(r), states.version.optional(r).toOption)
        }
    }

  }

  val localStates = new ConcurrentHashMap[UUID, State]

  def init(key: UUID, min: Int): TwFuture[State] =
    prepared.init(key, min)(CLevel.QUORUM).execAsTwitter()
      .map { _ =>
        val st = State(key, min, Map(), Map())
        localStates.put(key, st)
        st
      }

  def get(key: UUID): TwFuture[Option[State]] =
    prepared.get(key)(CLevel.QUORUM).execAsTwitter()

  def local(key: UUID): TwFuture[State] = {
    val st = localStates.computeIfAbsent(key, new Function[UUID,State] {
      override def apply(key: UUID) = new State(key)
    })
    if (st.expired) {
      st.update().map(_ => st)
    } else {
      TwFuture.value(st)
    }
  }

  def handle(op: BoundedCounterOp): TwFuture[CounterResult] = {
    import CounterOpType._
    val key = op.key.toUUID

    op.op match {

      case Init =>
        init(key, op.n.get.toInt) map { _ => CounterResult() }

      case Incr =>
        for {
          st <- local(key)
          _ <- st.incr(op.n.get.toInt)
        } yield {
          CounterResult()
        }

      case Decr =>
        for {
          st <- local(key)
          success <- st.decr(op.n.get.toInt)
        } yield {
          CounterResult(success = Some(success))
        }

      case Value =>
        for {
          st <- local(key)
        } yield {
          CounterResult(value = Some(st.value))
        }

      case EnumUnknownCounterOpType(e) =>
        throw ReservationException(s"Unknown op type: $e")
    }
  }

  class Handle(key: UUID) {
    import ipa.thrift.CounterOpType._
    def table = Table(space.name, name)

    def init(min: Int = 0): TwFuture[Unit] =
      reservations.client
          .boundedCounter(table, BoundedCounterOp(Init, key.toString, Some(min)))
          .unit

    def incr(by: Int = 1): TwFuture[Unit] =
      reservations.client
        .boundedCounter(table, BoundedCounterOp(Incr, key.toString, Some(by)))
        .unit

    def decr(by: Int = 1): TwFuture[Boolean] =
      reservations.client
        .boundedCounter(table, BoundedCounterOp(Decr, key.toString, Some(by)))
        .map(_.success.get)

    def value(): TwFuture[Int] =
      reservations.client
          .boundedCounter(table, BoundedCounterOp(Value, key.toString))
          .map(_.value.get.toInt)
  }

  def apply(key: UUID) = new Handle(key)

}

object BoundedCounter {

  case class DecrementException(value: Int)
      extends RuntimeException(s"Unable to decrement, already at minimum ($value).")

  def pack(i: Int, j: Int) = (i.toLong << 32) | (j & 0xffffffffL)
  def unpack(ij: Long) = ((ij >> 32).toInt, ij.toInt)
}
