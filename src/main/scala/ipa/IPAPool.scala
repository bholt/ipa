package ipa

import java.util.UUID

import com.websudos.phantom.dsl.{UUID, _}
import owl._
import owl.Util._
import com.datastax.driver.core.{ConsistencyLevel => CLevel}
import com.twitter.util.{Future => TwFuture}
import org.apache.commons.lang.NotImplementedException
import owl.Consistency._

abstract class IPAPool(val name: String)(implicit val imps: CommonImplicits)
    extends DataType(imps)
{ self: IPAPool.Bounds =>

  def counter: BoundedCounter with BoundedCounter.Bounds

  class Handle(val key: UUID) {
    def init(capacity: Int) = self.init(key, capacity)
    def take(n: Int = 1) = self.take(key, n)
    def remaining() = self.remaining(key)
  }

  def apply(key: UUID) = new Handle(key)

  override def create() = counter.create()
  override def truncate() = counter.truncate()

  def generate(n: Int, success: Boolean): Seq[UUID] =
    if (success) 0 until n map { _ => UUID.randomUUID() }
    else Seq()

}

object IPAPool {

  trait Bounds { self: IPAPool =>
    type ReadType[T] <: Inconsistent[T]
    type TakeType[T] <: Inconsistent[T]

    def init(key: UUID, capacity: Int): TwFuture[Unit] = {
      val handle = counter(key)
      for {
        _ <- handle.init()
        _ <- handle.incr(capacity)
      } yield ()
    }

    def take(key: UUID, n: Int): TwFuture[TakeType[Seq[UUID]]]
    def remaining(key: UUID): TwFuture[ReadType[Int]]
  }

  trait StrongBounds extends Bounds { self: IPAPool =>
    type ReadType[T] = Consistent[T]
    type TakeType[T] = Consistent[T]

    val bc = new BoundedCounter(name+"_bc") with BoundedCounter.StrongBounds

    override def counter = bc

    override def take(key: UUID, n: Int) =
      bc(key).decr(n).map(r => Consistent(generate(n, r.get)))

    override def remaining(key: UUID) =
      bc(key).value()
  }

  trait WeakBounds extends Bounds { self: IPAPool =>
    type ReadType[T] = Inconsistent[T]
    type TakeType[T] = Inconsistent[T]

    val bc = new BoundedCounter(name+"_bc") with BoundedCounter.WeakBounds

    override def counter = bc

    override def take(key: UUID, n: Int) =
      bc(key).decr(n).map(r => Inconsistent(generate(n, r.get)))

    override def remaining(key: UUID) =
      bc(key).value()
  }

  trait ErrorBound extends Bounds { pool: IPAPool =>
    type ReadType[T] = Interval[T]
    type TakeType[T] = Inconsistent[T]

    def bound: Tolerance

    lazy val bc = new BoundedCounter(name+"_bc")
        with BoundedCounter.ErrorBound { override val bound = pool.bound }

    override def counter = bc

    override def take(key: UUID, n: Int) =
      bc(key).decr(n).map(_.map(generate(n, _)))

    override def remaining(key: UUID) =
      bc(key).value()
  }

  trait LatencyBound extends Bounds { pool: IPAPool =>
    type ReadType[T] = Rushed[T]
    type TakeType[T] = Rushed[T]

    def bound: Latency

    lazy val bc = new BoundedCounter(name+"_bc")
        with BoundedCounter.LatencyBound { override val bound = pool.bound }

    override def counter = bc

    override def take(key: UUID, n: Int): TwFuture[Rushed[Seq[UUID]]] =
      bc(key).decr(n).map(_.map(generate(n, _)))

    override def remaining(key: UUID): TwFuture[Rushed[Int]] =
      bc(key).value()
  }

  def fromNameAndBound(name: String, bound: Bound)(implicit imps: CommonImplicits): IPAPool with Bounds = bound match {
    case l @ Latency(_) =>
      new IPAPool(name) with LatencyBound { override val bound = l }

    case Consistency(Weak, Weak) =>
      new IPAPool(name) with WeakBounds

    case Consistency(Weak, Strong) =>
      new IPAPool(name) with WeakBounds

    case Consistency(Strong, _) =>
      new IPAPool(name) with StrongBounds

    case t @ Tolerance(_) =>
      new IPAPool(name) with ErrorBound { override val bound = t }

    case e =>
      Console.err.println(s"Error creating BoundedCounter from bound: $e")
      sys.error(s"impossible case: $e")
  }
}
