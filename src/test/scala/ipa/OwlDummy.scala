package ipa

import com.twitter.util.{Future => TwFuture, Promise => TwPromise}
import com.websudos.phantom.dsl._
import ipa.adts.IPACounter
import ipa.Util._

import scala.language.postfixOps

//
//abstract class BaseSettings(implicit val space: KeySpace, val session: Session, val cassandraOpMetric: Timer, val ipa_metrics: IPAMetrics) {
//  def name: String
//}
//
//object Op {
//
//  trait Add {
//    def add(key: UUID, value: UUID): Future[Unit]
//  }
//
//  trait Contains {
//    type ContainsType
//    def contains(key: UUID, value: UUID): Future[ContainsType]
//  }
//
//  trait Size {
//    type SizeType
//    def size(key: UUID): Future[SizeType]
//  }
//
//  trait All extends Add with Contains with Size
//}
//
//
//trait SetHandle { self: Op.Add with Op.Contains with Op.Size =>
//  class Handle(key: UUID) {
//    def add(value: UUID): Future[Unit] = self.add(key, value)
//    def contains(value: UUID): Future[ContainsType] = self.contains(key, value)
//    def size(): Future[SizeType] = self.size(key)
//  }
//  def apply(key: UUID) = new Handle(key)
//}
//
//trait SetBase extends BaseSettings with SetHandle with TableGenerator {
//  self: Op.Add with Op.Contains with Op.Size =>
//
//  type K = UUID
//  type V = UUID
//
//  case class Entry(key: K, value: V)
//  class EntryTable extends CassandraTable[EntryTable, Entry] {
//    object ekey extends PrimitiveColumn[EntryTable, Entry, K](this) with PartitionKey[K]
//    object evalue extends PrimitiveColumn[EntryTable, Entry, V](this) with PrimaryKey[V]
//    override val tableName = name
//    override def fromRow(r: Row) = Entry(ekey(r), evalue(r))
//  }
//
//  val entryTable = new EntryTable
//
//  override def create(): Future[Unit] =
//    entryTable.create.ifNotExists.future().unit
//
//  override def truncate(): Future[Unit] =
//    entryTable.truncate.future().unit
//
//
//  def add(cons: ConsistencyLevel)(key: K, value: V): Future[Unit] = {
//    entryTable.insert()
//        .consistencyLevel_=(cons)
//        .value(_.ekey, key)
//        .value(_.evalue, value)
//        .future()
//        .instrument()
//        .unit
//  }
//
//  def contains(cons: ConsistencyLevel)(key: K, value: V): Future[Inconsistent[Boolean]] = {
//    entryTable.select(_.evalue)
//        .consistencyLevel_=(cons)
//        .where(_.ekey eqs key)
//        .and(_.evalue eqs value)
//        .one()
//        .instrument()
//        .map(o => Inconsistent(o.isDefined))
//  }
//
//  def size(cons: ConsistencyLevel)(key: UUID): Future[Inconsistent[Int]] = {
//    entryTable.select.count()
//        .consistencyLevel_=(cons)
//        .where(_.ekey eqs key)
//        .one()
//        .map(o => Inconsistent(o.getOrElse(0l).toInt))
//        .instrument()
//  }
//
//}
//
//trait RushImpl { this: BaseSettings =>
//  def rush[T](latencyBound: FiniteDuration)(op: ConsistencyLevel => Future[Inconsistent[T]]): Future[Rushed[T]] = {
//    val deadline = latencyBound.fromNow
//
//    val ops =
//      Seq(ConsistencyLevel.ALL, ConsistencyLevel.ONE) map { c =>
//        op(c) map { r => Rushed(r.get, c) }
//      }
//
//    ops.firstCompleted flatMap { r1 =>
//      val timeRemaining = deadline.timeLeft
//      if (r1.consistency == ConsistencyLevel.ALL ||
//          timeRemaining < config.assumed_latency) {
//        if (deadline.isOverdue()) ipa_metrics.missedDeadlines.mark()
//        Future(r1)
//      } else {
//        // make sure it finishes within the deadline
//        val fallback = Future {
//          blocking { Thread.sleep(timeRemaining.toMillis) }
//          r1
//        }
//        (ops.filterNot(_.isCompleted) :+ fallback)
//            .firstCompleted
//            .map { r2 => r1 max r2 } // return the higher-consistency one
//      }
//    }
//  }
//}
//
//trait RushedSize extends Op.Size with RushImpl { base: SetBase =>
//  def sizeBound: FiniteDuration
//
//  override type SizeType = Rushed[Int]
//
//  override def size(key: UUID): Future[SizeType] =
//    rush(sizeBound){ c: ConsistencyLevel => base.size(c)(key) }
//
//}
//
//trait RushedContains extends Op.Contains with RushImpl { base: SetBase =>
//  def containsBound: FiniteDuration
//
//  override type ContainsType = Rushed[Boolean]
//
//  override def contains(key: UUID, value: UUID): Future[ContainsType] =
//    rush(containsBound){ c: ConsistencyLevel => base.contains(c)(key, value) }
//
//}
//
//
//trait IntervalSize extends Op.Size {
//  def sizeBound: Tolerance
//
//  override type SizeType = Interval[Int]
//
//  override def size(key: UUID): Future[SizeType] =
//    Future(Interval(0, 1))
//
//}
//
//trait WeakAdd extends Op.Add { base: SetBase =>
//  override def add(key: UUID, value: UUID): Future[Unit] =
//    base.add(ConsistencyLevel.ONE)(key, value)
//}
//
//trait StrongAdd extends Op.Add { base: SetBase =>
//  override def add(key: UUID, value: UUID): Future[Unit] =
//    base.add(ConsistencyLevel.ALL)(key, value)
//}

/** Dummy tests (don't run as part of default test suite) */
class OwlDummy extends {
  override implicit val space = KeySpace("owl_dummy")
} with OwlWordSpec with IPAService {

//  case class State() extends FutureSerializer {
//    var counter = 0
//  }
  import ipa.adts.BoundedCounter.{pack,unpack}

  createKeyspace()

  def check(i: Int, j: Int) = {
    assert(unpack(pack(i, j)) == (i, j))
  }

  "Index handles all cases" in {
    check(5, 7)
    check(4, -1)
    check(Int.MaxValue, 7)
    check(Int.MinValue, -2)
    check(Int.MaxValue, Int.MaxValue)
    check(Int.MaxValue, Int.MinValue)
    check(Int.MinValue, Int.MaxValue)
    check(Int.MinValue, Int.MinValue)
    check(7, Int.MaxValue)
    check(-10, Int.MaxValue)
    check(-2, Int.MinValue)
    check(8, Int.MinValue)
  }

  "Dummy runs" in {

    val counter = new IPACounter("counter") with IPACounter.StrongOps

    counter.createTwitter().await()

//    val st = State()

//    def work(i: Int): TwFuture[Int] = {
//      Console.err.println(s"started $i (${st.counter})")
//      counter.incrTwitter(Consistency.Strong)(1.id, 1) map { _ =>
//        st.counter += 1
//        Console.err.println(s"finished $i (${st.counter})")
//        st.counter
//      }
//    }
//
//    st submit { work(1) }
//    st submit { work(2) }
//    st submit { work(3) }
//
//
////    val myset = new SetBase with RushedSize with RushedContains with WeakAdd {
//      val name = "myset"
//      val sizeBound = 50 millis
//      val containsBound = 50 millis
//    }
//
//    val test = myset(0.id).size().futureValue
//    println(s"${test.get} with ${test.consistency}")
//
//    val iset = new SetBase with IntervalSize with RushedContains with StrongAdd {
//      val name = "myset"
//      val sizeBound = Tolerance(0.01)
//      val containsBound = 50 millis
//    }
//
//    val v2 = iset(0.id).size().futureValue
//    println(s"${v2.get} with ${v2.max}")
  }
}
