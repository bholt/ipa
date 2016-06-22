~~~scala
package owl

import com.datastax.driver.core.{Row, ConsistencyLevel}
import com.websudos.phantom.builder.primitives.Primitive
import com.websudos.phantom.keys.PartitionKey

import scala.concurrent._
import scala.concurrent.duration._
import com.websudos.phantom.dsl._
import scala.language.postfixOps

import Util._

abstract class MySet[K, V](val name: String)
    (implicit val evK: Primitive[K], val evV: Primitive[V], val session: Session, val space: KeySpace) {

  def create(): Future[Unit]
  def truncate(): Future[Unit]

  def add(key: K, value: V): Future[Unit]

  class Handle(key: K) {
    def add(value: V) = MySet.this.add(key, value)
  }

  def apply(key: K) = new Handle(key)

  def consistency = ConsistencyLevel.ALL
}

object MySet {

  trait PlainImpl[K, V] { this: MySet[K, V] =>

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


    override def add(key: K, value: V): Future[Unit] = {
      entryTable.insert()
          .consistencyLevel_=(consistency)
          .value(_.ekey, key)
          .value(_.evalue, value)
          .future()
          .unit
    }

    def size(key: K): Future[Int] = Future { 1 }

    class PlainHandle(key: K) extends Handle(key) {
      def size() = PlainImpl.this.size(key)
    }
    override def apply(key: K) = new PlainHandle(key)
  }

  trait LatencyBound[K, V] { self: MySet[K, V] with PlainImpl[K, V] =>
    val bound: Duration

    override def size(key: K): Int = {



      println(s"must return in $bound")
      2
    }
  }


}

/** Dummy tests (don't run as part of default test suite) */
class OwlDummy extends {
  override implicit val space = KeySpace("owl_dummy")
} with OwlTest {

  "Dummy" should "run" in {
    implicit val ec = boundedQueueExecutionContext(capacity = config.cap)

    val duration = 10.seconds
    println(s"# running workload for $duration, with ${config.cap} at a time")
    val deadline = duration.fromNow

    val stream =
      Stream from 1 map { i =>
        println(s"[$i] created")
        Future {
          println(s"[$i] executing")
          blocking {
            Thread.sleep(1.second.toMillis)
          }
          println(s"[$i] done")
        }
      } takeWhile { _ =>
        deadline.hasTimeLeft
      } bundle

    await(stream)

    println("####")
  }

  val u1 = User.id(1)
  val u2 = User.id(2)
  object testSet extends { val bound = 1 second }
      with MySet[UUID,UUID]("set")
      with MySet.PlainImpl[UUID, UUID]
      with MySet.LatencyBound[UUID, UUID]

  it should "make IPASet" in {
    testSet(u1).add(u2)
    testSet(u1).size()
  }
}
~~~
