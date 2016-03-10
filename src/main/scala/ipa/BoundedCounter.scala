package ipa

import java.util.UUID

import com.datastax.driver.core.{ConsistencyLevel => CLevel}
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.dsl._
import com.twitter.util.{Future => TwFuture, Promise => TwPromise}
import ipa.thrift.{BoundedCounterOp, CounterResult, Table}
import owl.Connector
import owl.Connector.config
import owl.Util._

import scala.collection.mutable
import scala.collection.concurrent

class BoundedCounter(val name: String)(implicit val imps: CommonImplicits) extends DataType(imps) {
  import BoundedCounter._

  val me: Int = this_host_hash

  class State(val key: UUID, var min: Int = 0) {
    var lastReadAt = 0L
    val rights = new mutable.HashMap[(Int, Int), Int].withDefaultValue(0)
    val consumed = new mutable.HashMap[Int, Int].withDefaultValue(0)

    override def toString = s"($key -> min: $min, rights: $rights, consumed: $consumed)"

    def expired: Boolean = (System.nanoTime - lastReadAt) > config.lease.periodNanos

    def value: Int =
      min - consumed.values.sum +
          rights.map{
            case ((i,j),v) if i == j => v
            case _ => 0
          }.sum

    def myRights: Int =
      rights((me,me)) - consumed(me) +
          rights.map{
            case ((i,j),v) if i != me && j == me => v
            case ((i,j),v) if i == me && j != me => -v
            case _ => 0
          }.sum

    def update(): TwFuture[Unit] = {
      prepared.get(key)(CLevel.QUORUM).execAsTwitter() map {
        case Some(st) =>
          min = st.min
          rights ++= st.rights
          consumed ++= st.consumed
        case _ =>
          sys.error(s"Unable to get State($key)")
      }
    }

    def incr(n: Int): TwFuture[Unit] = {
      val v = rights((me, me)) + n
      rights += ((me, me) -> v)
      prepared.set(key, me, me, v)(CLevel.ONE).execAsTwitter()
    }

    def decr(n: Int) = {
      require(myRights >= n)
      val v = consumed(me) + n
      consumed += (me -> v)
      prepared.consume(key, me, v)(CLevel.ONE).execAsTwitter()
    }

    def transfer(n: Int, to: Int, promise: TwPromise[Int] = null, retries: Int = 0): TwFuture[Int] = {
      require(myRights >= n)
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

    def balance(promise: TwPromise[Unit] = null) = {
      val pr = if (promise != null) promise else TwPromise[Unit]()

      

      pr
    }

    def merge() = ???
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
    object state extends MapColumn[StateTable, State, Long, Int](this)
    object consumed extends MapColumn[StateTable, State, Int, Int](this)

    override val tableName = name
    override def fromRow(r: Row) = State(key(r), min(r), state(r), consumed(r))
  }

  val states = new StateTable

  override def create() = states.create.ifNotExists().future().unit
  override def truncate() = states.truncate().future().unit

  object prepared {
    private val (t, k, s, c) = (s"${space.name}.${states.tableName}", states.key.name, states.state.name, states.consumed.name)

    lazy val init: (UUID, Int) => (CLevel) => BoundOp[Unit] = {
      val ps = session.prepare(
        s"UPDATE $t SET ${states.min.name}=?, $s={}, $c={} WHERE $k=?")
      (key: UUID, min: Int) => ps.bindWith(min, key)(_ => ())
    }

    lazy val get: (UUID) => (CLevel) => BoundOp[Option[State]] = {
      val ps = session.prepare(s"SELECT * FROM $t WHERE $k = ?")
      (key: UUID) =>
        ps.bindWith(key)(_.first.map(states.fromRow))
    }

    lazy val set: (UUID, Int, Int, Int) => (CLevel) => BoundOp[Unit] = {
      Console.err.println(s"set table: $t")
      val ps = session.prepare(s"UPDATE $t SET $s=$s+? WHERE $k = ?")
      (key: UUID, i: Int, j: Int, v: Int) => {
        ps.bindWith(Map(pack(i,j) -> v), key)(_ => ())
      }
    }

    lazy val transfer: (UUID, (Int, Int), Int, Int) => (CLevel) => BoundOp[Boolean] = {
      val ps = session.prepare(s"UPDATE $t SET $s=$s+? WHERE $k = ? IF $s[?] = ?")
      (key: UUID, ij: (Int, Int), v: Int, prev: Int) => {
        val pij = pack(ij._1, ij._2)
        ps.bindWith(Map(pij -> v), key, pij, prev) {
          _.first.exists(_.get(0, classOf[Boolean]))
        }
      }
    }

    lazy val consume: (UUID, Int, Int) => (CLevel) => BoundOp[Unit] = {
      val ps = session.prepare(s"UPDATE $t SET $c=$c+? WHERE $k=?")
      (key: UUID, i: Int, v: Int) => ps.bindWith(Map(i -> v), key)(_ => ())
    }

  }

  val localStates = new concurrent.TrieMap[UUID, State]

  def init(key: UUID, min: Int): TwFuture[State] =
    prepared.init(key, min)(CLevel.QUORUM).execAsTwitter()
      .map { _ =>
        val st = State(key, min, Map(), Map())
        localStates += (key -> st)
        st
      }

  def get(key: UUID): TwFuture[Option[State]] =
    prepared.get(key)(CLevel.QUORUM).execAsTwitter()

  def local(key: UUID): TwFuture[State] = {
    val st = localStates.getOrElseUpdate(key, new State(key))
    if (st.expired) {
      st.update().map(_ => st)
    } else {
      TwFuture.value(st)
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

    def decr(by: Int = 1): TwFuture[Unit] =
      reservations.client
        .boundedCounter(table, BoundedCounterOp(Decr, key.toString, Some(by)))
        .unit

    def value(): TwFuture[Long] =
      reservations.client
          .boundedCounter(table, BoundedCounterOp(Value, key.toString))
          .map(_.value.get)
  }

  def apply(key: UUID) = new Handle(key)

}

object BoundedCounter {

  def pack(i: Int, j: Int) = (i.toLong << 32) | (j & 0xffffffffL)
  def unpack(ij: Long) = ((ij >> 32).toInt, ij.toInt)
}
