package owl

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.concurrent.locks.{Lock, ReentrantLock, ReentrantReadWriteLock}
import java.util.concurrent.{ConcurrentHashMap, Semaphore}
import java.util.function.Function

import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import ipa.IPACounter
import org.apache.commons.math3.distribution.ZipfDistribution
import owl.Util._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

class RawMixCounter(val duration: FiniteDuration) extends {
  override implicit val space = RawMix.space
} with OwlService {
  import Consistency._
  import RawMixCounter.truth

  val nsets = config.rawmix.nsets
  val mix = config.rawmix.counter.mix

  val zipfDist = new ZipfDistribution(nsets, config.zipf)

  def zipfID() = id(zipfDist.sample())
  def urandID() = id(Random.nextInt(nsets))

  val counter = IPACounter.fromNameAndBound("raw", config.bound)

  val timerIncr = metrics.create.timer("incr_latency")
  val timerRead = metrics.create.timer("read_latency")

  val countReadStrong = metrics.create.counter("read_strong")
  val countReadWeak   = metrics.create.counter("read_weak")

  val histIntervalWidth = metrics.create.histogram("interval_width")
  val histIntervalPercent = metrics.create.histogram("interval_percent")
  val countCorrect = metrics.create.counter("correct")
  val countIncorrect = metrics.create.counter("incorrect")
  val countContains = metrics.create.counter("contains")
  val countNotContains = metrics.create.counter("contains_not")
  val histError = metrics.create.histogram("error")
  val countErrorNegative = metrics.create.counter("error_negative")

  def recordResult(trueVal: Long, inflight: Long, r: Inconsistent[Long]): Inconsistent[Long] = {

    // some require special additional handling...
    r match {
      case v: Interval[Long] =>
        val width = v.max - v.min
        histIntervalWidth << width
        histIntervalPercent << (width / v.median * 10000).toLong

        if (v.min <= trueVal && v.max >= (trueVal+inflight)) countCorrect += 1
        else countIncorrect += 1

        if (v.contains(trueVal)) countContains += 1 else countNotContains += 1
        histError << Math.abs(v.median - trueVal).toLong
        if (v.median < trueVal) countErrorNegative += 1

      case v =>
        if (v.get >= trueVal && v.get <= (trueVal+inflight)) countCorrect += 1
        else countIncorrect += 1

        histError << Math.abs(v.get - trueVal)
        histIntervalWidth << inflight
        if (v.get < trueVal) countErrorNegative += 1
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
      val t = truth(handle.key)
      val f = op match {
        case 'incr =>
          t.inflight.incrementAndGet()
          handle.incr().instrument(timerIncr)
                .map { _ => t.incr(); t.inflight.decrementAndGet() }
        case 'read =>
          val t = truth(handle.key)
          val trueVal = t.get
          val f = t.inflight.get
          handle.read().instrument(timerRead)
              .map(recordResult(trueVal, Math.max(t.inflight.get, f), _)).unit
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

  case class Truth(v: AtomicLong = new AtomicLong,
      inflight: AtomicLong = new AtomicLong) {
    def incr() = v.incrementAndGet()
    def get = v.get()
  }


  val truthValues = new ConcurrentHashMap[UUID, Truth]()
  def truth(key: UUID): Truth = {
    truthValues.computeIfAbsent(key, new Function[UUID,Truth] {
      override def apply(key: UUID) = Truth()
    })
  }

  def main(args: Array[String]): Unit = {
    if (config.do_reset) dropKeyspace()
    createKeyspace()

    val warmup = new RawMixCounter(5 seconds)
    println(s">>> warmup (${warmup.duration})")
    warmup.run(truncate = true)

    reservations.clients.values.map(_.metricsReset()).bundle().await()

    val workload = new RawMixCounter(config.duration)
    println(s">>> workload (${workload.duration})")
    workload.run()
    workload.metrics.dump()

    sys.exit()
  }

}
