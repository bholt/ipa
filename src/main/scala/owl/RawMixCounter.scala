package owl

import java.util.concurrent.Semaphore

import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import ipa.IPACounter
import org.apache.commons.math3.distribution.ZipfDistribution
import owl.Util._

import scala.concurrent.duration._
import scala.util.Random

class RawMixCounter(val duration: FiniteDuration) extends {
  override implicit val space = RawMix.space
} with OwlService {
  import Consistency._

  val nsets = config.rawmix.nsets
  val mix = config.rawmix.counter.mix

  val zipfDist = new ZipfDistribution(nsets, config.zipf)

  def zipfID() = id(zipfDist.sample())
  def urandID() = id(Random.nextInt(nsets))

  val counter = IPACounter.fromBound(config.bound)

  val timerIncr = metrics.create.timer("incr_latency")
  val timerRead = metrics.create.timer("read_latency")

  val countReadStrong = metrics.create.counter("read_strong")
  val countReadWeak   = metrics.create.counter("read_weak")

  val countConsistent = metrics.create.counter("consistent")
  val countInconsistent = metrics.create.counter("inconsistent")

  val histIntervalWidth = metrics.create.histogram("interval_width")

  def recordResult(r: Inconsistent[Long]): Inconsistent[Long] = {
    // some require special additional handling...
    counter match {
      case _: IPACounter.ErrorTolerance =>
        val iv = r.asInstanceOf[Interval[Long]]
        val width = iv.max - iv.min
        histIntervalWidth << width
      case _ => // do nothing
    }
    r.consistency match {
      case Strong => countReadStrong += 1
      case Weak   => countReadWeak += 1
      case _ => // do nothing
    }
    r
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
