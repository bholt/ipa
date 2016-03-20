package owl

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.concurrent.locks.{Lock, ReentrantLock, ReentrantReadWriteLock}
import java.util.concurrent.{ConcurrentHashMap, Semaphore, TimeUnit}
import java.util.function.Function

import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import ipa.{BoundedCounter, IPACounter}
import org.apache.commons.math3.distribution.ZipfDistribution
import owl.Util._

import com.twitter.util.Future
import scala.concurrent.duration._
import scala.util.Random
import scala.collection.mutable
import Connector.config.rawmix

import Console.{err => log}

class RawMixCounter(val duration: FiniteDuration) extends {
  override implicit val space = RawMix.space
} with OwlService {
  import Consistency._
  import RawMixCounter.truth

  val nsets = rawmix.nsets
  val mix = rawmix.counter.mix

  val target = rawmix.target

  val zipfDist = new ZipfDistribution(nsets, config.zipf)

  def zipfID() = id(zipfDist.sample())
  def urandID() = id(Random.nextInt(nsets))

  val counter = BoundedCounter.fromNameAndBound("raw", config.bound)

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

  def generate(): Unit = {

    counter.create().await()
    counter.truncate().await()

    log.println(s"# initializing $nsets counters")
    (0 to nsets)
        .map { i =>
          val n = Random.nextInt(target / 4)
          truth(i.id).v.set(n)
          counter(i.id).init(0).flatMap(_ => counter(i.id).incr(n))
        }
        .bundle().await()
  }

  def run() {

    val actualDurationStart = Deadline.now
    val deadline = duration.fromNow
    val sem = new Semaphore(config.concurrent_reqs)

    log.println(s"# starting experiments")

    val keys = mutable.ArrayBuffer[Int](0 until nsets :_*)
    val maxkey = new AtomicInteger(nsets + 1)

    while (deadline.hasTimeLeft &&
      sem.tryAcquire(deadline.timeLeft.inMillis, TimeUnit.MILLISECONDS)) {
      val i = Random.nextInt(keys.size)
      val k = keys(i)
      val key = k.id
      val handle = counter(key)

      val tval = truth(key).v.incrementAndGet()

      val f = if (tval > target) {
        Future.Unit
      } else {
        handle.incr().instrument(timerIncr) flatMap { _ =>
          if (tval != target) {
            Future.Unit
          } else {
            handle.value() map {
              case r: Interval[Int] =>
                if (r.contains(target)) {
                  countCorrect += 1
                  countContains += 1
                } else {
                  countIncorrect += 1
                }
                log.println(s"# [$k] truth = $tval, got = $r")

              case r: Inconsistent[Int] =>
                val v = r.get
                if (v == target) countCorrect += 1
                else countIncorrect += 1

                histError << Math.abs(target - r.get)
                if (r.get < target) countErrorNegative += 1
                log.println(s"# [$k] truth = $tval, got = $r")

              case e =>
                log.println(s"!! unhandled case: $e")

            } flatMap { _ =>
              // now replace this key with a new one
              val j = maxkey.getAndIncrement()
              counter(j.id).init(0) map { _ => keys(i) = j }
              // log.println(s"# starting ${keys(i)}")
            }
          }
        }
      }

      f onSuccess { case _ => sem.release() }
      f onFailure { case e: Throwable =>
        Console.err.println(s"!! got an error: ${e.getMessage}")
        e.printStackTrace()
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

//    val warmup = new RawMixCounter(5 seconds)
//    println(s">>> warmup (${warmup.duration})")
//    warmup.run()
//
//    println(">>> resetting metrics")
//    reservations.clients.values.map(_.metricsReset()).bundle().await()

    println(">>> creating new RawMixCounter")
    val workload = new RawMixCounter(config.duration)

    workload.generate()

    println(s">>> workload (${workload.duration})")
    workload.run()
    workload.metrics.dump()

    sys.exit()
  }

}
