package ipa

import java.util.UUID

import com.websudos.phantom.dsl._
import owl._
import owl.Util._
import com.datastax.driver.core.{ConsistencyLevel => CLevel}
import com.twitter.util.{Future => TwFuture}

abstract class IPAPool(val name: String)(implicit val imps: CommonImplicits)
    extends DataType(imps)
{ self: IPAPool.Ops =>

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

  trait Ops { self: IPAPool =>
    type IPAType[T] <: Inconsistent[T]

    def init(key: UUID, capacity: Int): TwFuture[Unit] = {
      val handle = counter(key)
      for {
        _ <- handle.init()
        _ <- handle.incr(capacity)
      } yield ()
    }

    def take(key: UUID, n: Int): TwFuture[IPAType[Seq[UUID]]]
    def remaining(key: UUID): TwFuture[IPAType[Int]]
  }

  trait StrongBounds extends Ops { self: IPAPool =>
    type IPAType[T] = Consistent[T]
    val bc = new BoundedCounter(name+"_bc") with BoundedCounter.StrongBounds

    override def counter = bc

    override def take(key: UUID, n: Int) =
      bc(key).decr(n).map(r => Consistent(generate(n, r.get)))

    override def remaining(key: UUID) =
      bc(key).value()
  }

  trait WeakBounds extends Ops { self: IPAPool =>
    type IPAType[T] = Inconsistent[T]
    val bc = new BoundedCounter(name+"_bc") with BoundedCounter.WeakBounds

    override def counter = bc

    override def take(key: UUID, n: Int) =
      bc(key).decr(n).map(r => Inconsistent(generate(n, r.get)))

    override def remaining(key: UUID) =
      bc(key).value()
  }

}
