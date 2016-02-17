package owl

import java.util.UUID

import com.datastax.driver.core.ConsistencyLevel
import nl.grons.metrics.scala.Timer

import scala.concurrent._
import scala.concurrent.duration._
import com.websudos.phantom.dsl._
import scala.language.postfixOps
import scala.math.Ordering.Implicits._

import Util._
import Connector.config


abstract class BaseSettings(implicit val space: KeySpace, val session: Session, val cassandraOpMetric: Timer, val ipa_metrics: IPAMetrics) {
  def name: String
}

trait SetBase extends BaseSettings with Set with TableGenerator with SizeImpl {

  type K = UUID
  type V = UUID

  case class Entry(key: K, value: V)
  class EntryTable extends CassandraTable[EntryTable, Entry] {
    object ekey extends PrimitiveColumn[EntryTable, Entry, K](this) with PartitionKey[K]
    object evalue extends PrimitiveColumn[EntryTable, Entry, V](this) with PrimaryKey[V]
    override val tableName = name
    override def fromRow(r: Row) = Entry(ekey(r), evalue(r))
  }

  val entryTable = new EntryTable

  override def create(): Future[Unit] =
    entryTable.create.ifNotExists.future().unit

  override def truncate(): Future[Unit] =
    entryTable.truncate.future().unit

  def size(cons: ConsistencyLevel)(key: UUID): Future[Inconsistent[Int]] = {
    entryTable.select.count()
        .consistencyLevel_=(cons)
        .where(_.ekey eqs key)
        .one()
        .map(o => Inconsistent(o.getOrElse(0l).toInt))
        .instrument()
  }

}

trait SizeImpl {
  type SizeType
  def size(key: UUID): Future[SizeType]
}

trait RushImpl { this: BaseSettings =>
  def rush[T](latencyBound: FiniteDuration)(op: ConsistencyLevel => Future[Inconsistent[T]]): Future[Rushed[T]] = {
    val deadline = latencyBound.fromNow

    val ops =
      Seq(ConsistencyLevel.ALL, ConsistencyLevel.ONE) map { c =>
        op(c) map { r => Rushed(r.get, c) }
      }

    ops.firstCompleted flatMap { r1 =>
      val timeRemaining = deadline.timeLeft
      if (r1.consistency == ConsistencyLevel.ALL ||
          timeRemaining < config.assumed_latency) {
        if (deadline.isOverdue()) ipa_metrics.missedDeadlines.mark()
        Future(r1)
      } else {
        // make sure it finishes within the deadline
        val fallback = Future {
          blocking { Thread.sleep(timeRemaining.toMillis) }
          r1
        }
        (ops.filterNot(_.isCompleted) :+ fallback)
            .firstCompleted
            .map { r2 => r1 max r2 } // return the higher-consistency one
      }
    }
  }
}

trait RushedSize extends SizeImpl with RushImpl { base: SetBase =>
  def sizeBound: FiniteDuration

  override type SizeType = Rushed[Int]

  override def size(key: UUID): Future[SizeType] =
    rush(sizeBound){ c: ConsistencyLevel => base.size(key) }

}

trait IntervalSize extends SizeImpl {
  def sizeBound: Tolerance

  override type SizeType = Interval[Int]

  override def size(key: UUID): Future[SizeType] =
    Future(Interval(0, 1))

}

trait Set { self: SizeImpl =>
  class Handle(key: UUID) {
    def size(): Future[SizeType] = self.size(key)
  }
  def apply(key: UUID) = new Handle(key)
}


/** Dummy tests (don't run as part of default test suite) */
class OwlDummy extends {
  override implicit val space = KeySpace("owl_dummy")
} with OwlTest {

  "Dummy" should "run" in {


    val myset = new SetBase with RushedSize {
      val name = "myset"
      val sizeBound = 50 millis
    }

    val test = myset(0.id).size().futureValue
    println(s"${test.get} with ${test.consistency}")

    val iset = new SetBase with IntervalSize {
      val name = "myset"
      val sizeBound = Tolerance(0.01)
    }

    val v2 = iset(0.id).size().futureValue
    println(s"${v2.get} with ${v2.max}")
  }
}
