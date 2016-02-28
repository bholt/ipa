package owl

import com.datastax.driver.core.{ConsistencyLevel => CLevel}
import org.joda.time.DateTime

import scala.math.Ordering.Implicits._
import ipa.thrift
import com.twitter.{util => tw}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

/** Define ordering over consistency levels */
trait ConsistencyOrder extends Ordering[CLevel] {
  def compare(a: CLevel, b: CLevel) = a.compareTo(b)
}

object Consistency {
  val Strong = CLevel.ALL
  val Weak = CLevel.ONE
}

sealed trait Bound
final case class Latency(d: FiniteDuration) extends Bound
final case class Consistency(read: CLevel, write: CLevel) extends Bound
final case class Tolerance(error: Double) extends Bound {
  def delta(value: Long) = (value * error).toLong
}

class IPAType {}

class Inconsistent[T](value: T) extends IPAType {

  /** get the value anyway (should we call it 'endorse'?)*/
  def get: T = value

}
object Inconsistent { def apply[T](value: T) = new Inconsistent(value) }


case class Consistent[T](value: T) extends Inconsistent[T](value) {}


class Transient[T](value: T) extends Inconsistent[T](value) {
  /** wait for it to become consistent */
  def waitUntilConsistent(): T = {
    // TODO: implement me
    value
  }
}


class Rushed[T](value: T, cons: CLevel)
    extends Inconsistent[T](value) with Ordered[Rushed[T]]
{
  def consistency = cons
  def compare(o: Rushed[T]) = { this.consistency compareTo o.consistency }
  override def toString = s"Rushed($value, $consistency)"
}

object Rushed {
  def apply[T](value: T, c: CLevel) = new Rushed(value, c)
}


class Stale[T](
    value: T,
    override val consistency: CLevel,
    val time: DateTime
) extends Rushed[T](value, consistency)



class Interval[T](val min: T, val max: T)(implicit ev: Numeric[T]) extends Inconsistent[T](min) {
  override def get = median
  def median: T = { min } // FIXME
  def contains(o: T): Boolean = { o >= min && o <= max }
  override def toString = s"Interval($min..$max)"
}
object Interval {
  def apply[T](min: T, max: T)(implicit ev: Numeric[T]) = new Interval[T](min, max)
}

object Conversions {
  implicit def thriftIntervalLongToNative(v: thrift.IntervalLong): Interval[Long] =
    Interval[Long](v.min, v.max)

  implicit def thriftTwFutureToNative[A, B](f: tw.Future[A])(implicit ev: A => B): tw.Future[B] = f map { v => v: B }

  implicit def thriftFutureToNative[A, B](f: Future[A])(implicit ev: A => B, ec: ExecutionContext): Future[B] = f map { v => v: B }

  implicit def consistentValueToValue[T](c: Consistent[T]): T = c.get
  implicit def valueToConsistentValue[T](v: T): Consistent[T] = Consistent(v)
}
