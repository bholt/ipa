package owl

import java.util.UUID

import com.datastax.driver.core.{ConsistencyLevel => CLevel}
import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import org.apache.commons.math3.distribution.ZipfDistribution

import scala.concurrent._
import scala.concurrent.duration._
import Util._
import java.util.concurrent.Semaphore

import ipa.Counter.{StrongOps, WeakOps}
import ipa.{CommonImplicits, Counter}
import owl.Connector.config.bound._

import scala.util.{Random, Success, Try}

class RawMixCounter(val duration: FiniteDuration) extends {
  override implicit val space = RawMix.space
} with OwlService {
  val nsets = config.rawmix.nsets
  val mix = config.rawmix.counter.mix

  val zipfDist = new ZipfDistribution(nsets, config.zipf)

  def zipfID() = id(zipfDist.sample())
  def urandID() = id(Random.nextInt(nsets))

  val counter = config.bound.parsed match {
    case Latency(l) =>
      new Counter("raw") with Counter.LatencyBound { override val bound = l }
    case Consistency(CLevel.ONE) =>
      new Counter("raw") with Counter.WeakOps
    case Consistency(CLevel.ALL) =>
      new Counter("raw") with Counter.StrongOps
    case t @ Tolerance(_) =>
      new Counter("raw") with Counter.ErrorTolerance { override val tolerance = t }
    case e =>
      println("error parsing bound")
      sys.error(s"impossible case: $e")
  }

  val timerIncr      = metrics.create.timer("incr_latency")
  val timerRead = metrics.create.timer("read_latency")

  val countReadStrong = metrics.create.counter("read_strong")
  val countReadWeak   = metrics.create.counter("read_weak")

  val countConsistent = metrics.create.counter("consistent")
  val countInconsistent = metrics.create.counter("inconsistent")

  val histIntervalWidth = metrics.create.histogram("interval_width")

  def recordResult(r: Any): Inconsistent[Long] = {
    val cons = counter match {
      case _: Counter.ErrorTolerance =>
        val iv = r.asInstanceOf[Interval[Long]]
        val width = iv.max - iv.min
        histIntervalWidth << width
        CLevel.ONE // reads always weak

      case _: LatencyBound =>
        r.asInstanceOf[Rushed[Long]].consistency

      case cbound: ConsistencyBound =>
        cbound.consistencyLevel

      case _: WeakOps => Consistency.Weak
      case _: StrongOps => Consistency.Strong
      case _ =>
        sys.error("datatype didn't match any of the options")
    }
    cons match {
      case CLevel.ALL => countReadStrong += 1
      case CLevel.ONE => countReadWeak += 1
      case _ => // do nothing
    }
    r.asInstanceOf[Inconsistent[Long]]
  }

  def instrument[T, U](op: Symbol)(f: Future[T]): Future[T] = {
    op match {
      case 'incr => f.instrument(timerIncr)
      case 'read => f.instrument(timerRead)
    }
  }

  def run(truncate: Boolean = false) {

    counter.create().await()
    if (truncate) counter.truncate().await()

    val actualDurationStart = Deadline.now
    val deadline = duration.fromNow
    val sem = new Semaphore(config.concurrent_reqs)

    while (deadline.hasTimeLeft) {
      sem.acquire()
      val handle = counter(zipfID())
      val op = weightedSample(mix)
      val f = op match {
        case 'incr =>
          handle.incr().instrument(timerIncr)
        case 'read =>
          handle.read().instrument(timerRead).map(recordResult(_)).unit
      }
      f onSuccess { case _ => sem.release() }
      f onFailure { case e: Throwable =>
        Console.err.println(e.getMessage)
        sys.exit(1)
      }
    }

    val actualTime = actualDurationStart.elapsed
    output += ("actual_time" -> actualTime)
    println(s"# Done in ${actualTime.toSeconds}.${actualTime.toMillis%1000}s")
  }

}

object RawMixCounter extends {
  override implicit val space = KeySpace("rawmix")
} with Connector {

  def main(args: Array[String]): Unit = {
    if (config.do_reset) dropKeyspace()
    createKeyspace()

    val warmup = new RawMixCounter(5 seconds)
    println(s">>> warmup (${warmup.duration})")
    warmup.run(truncate = true)

    reservations.all map { _.metricsReset() } bundle() await()

    val workload = new RawMixCounter(config.duration)
    println(s">>> workload (${workload.duration})")
    workload.run()
    workload.metrics.dump()

    sys.exit()
  }

}
