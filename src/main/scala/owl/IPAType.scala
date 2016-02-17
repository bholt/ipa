package owl

import com.datastax.driver.core.ConsistencyLevel
import org.joda.time.DateTime
import scala.math.Ordering.Implicits._

/** Define ordering over consistency levels */
trait ConsistencyOrder extends Ordering[ConsistencyLevel] {
  def compare(a: ConsistencyLevel, b: ConsistencyLevel) = a.compareTo(b)
}

class IPAType {}

class Inconsistent[T](value: T) extends IPAType {

  /** get the value anyway (should we call it 'endorse'?)*/
  def get: T = value

}
object Inconsistent { def apply[T](value: T) = new Inconsistent(value) }


class Transient[T](value: T) extends Inconsistent[T](value) {
  /** wait for it to become consistent */
  def waitUntilConsistent(): T = {
    // TODO: implement me
    value
  }
}


class Rushed[T](value: T, cons: ConsistencyLevel)
    extends Inconsistent[T](value) with Ordered[Rushed[T]]
{
  def consistency = cons
  def compare(o: Rushed[T]) = { this.consistency compareTo o.consistency }
  override def toString = s"Rushed($value, $consistency)"
}

object Rushed {
  def apply[T](value: T, c: ConsistencyLevel) = new Rushed(value, c)
}


class Stale[T](
    value: T,
    override val consistency: ConsistencyLevel,
    val time: DateTime
) extends Rushed[T](value, consistency)


case class Tolerance(val error: Double)

class Interval[T](val min: T, val max: T)(implicit ev: Numeric[T]) extends Inconsistent[T](min) {
  override def get = median
  def median: T = { min } // FIXME
  def contains(o: T): Boolean = { o >= min && o <= max }
}
