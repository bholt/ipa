package owl

import scala.concurrent.duration.Duration

class IPAType {}

trait Inconsistent[T] extends IPAType {
  def get(): T
}

trait Stale[T] extends Inconsistent[T] {
  def olderThan(time: Duration): Boolean
}

trait Interval[T] extends Inconsistent[T] {
  def contains(value: T): Boolean
}
