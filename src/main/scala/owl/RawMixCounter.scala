package owl

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.concurrent.locks.{Lock, ReentrantLock, ReentrantReadWriteLock}
import java.util.concurrent.{ConcurrentHashMap, Semaphore, TimeUnit}
import java.util.function.Function

import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import ipa.{BoundedCounter, IPACounter}
import org.apache.commons.math3.distribution.{UniformIntegerDistribution, ZipfDistribution}
import owl.Util._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random
import scala.collection.mutable
import Connector.config.rawmix
import owl.RawMixCounter.Truth

import Console.{err => log}

class RawMixCounter(val duration: FiniteDuration) extends {
  override implicit val space = RawMix.space
} with OwlService {
  import Consistency._
  import RawMixCounter.truth

  val nsets = rawmix.nsets
  val mix = rawmix.counter.mix

  val dist = if (config.zipf == 0) {
    println("# Uniform distribution")
    new UniformIntegerDistribution(0, nsets-1)
  } else {
    println(s"# Zipf distribution: ${config.zipf}")
    new ZipfDistribution(nsets, config.zipf)
  }

  val counter = IPACounter.fromNameAndBound("raw", config.bound)

  val timerIncr = metrics.create.timer("incr_latency")
  val timerRead = metrics.create.timer("read_latency")

  val countReadStrong = metrics.create.counter("read_strong")
  val countReadWeak   = metrics.create.counter("read_weak")

  val histIntervalWidth = metrics.create.histogram("interval_width")
  val histIntervalPercent = metrics.create.histogram("interval_percent")
  val countCorrect = metrics.create.counter("correct")
  val countIncorrect = metrics.create.counter("incorrect")
  val countEventuallyCorrect = metrics.create.counter("eventually_correct")
  val countContains = metrics.create.counter("contains")
  val countNotContains = metrics.create.counter("contains_not")
  val histError = metrics.create.histogram("error")
  val countErrorNegative = metrics.create.counter("error_negative")

  def init(i: Int, key: UUID): Future[Unit] = {
    val t = Random.nextInt(rawmix.target) + rawmix.target / 4
    val scaled = (t * nsets * dist.probability(i)).toInt + 4
    val initial = scaled / 2
    Truth.init(key, scaled, initial)
    counter(key).incr(initial)
  }

  def generate(): Unit = {

    counter.create().await()
    counter.truncate().await()

    // log.println(s"# initializing $nsets counters")
    (0 to nsets).map(i => init(i, i.id)).bundle.await()
  }

  def run() {

    val actualDurationStart = Deadline.now
    val deadline = duration.fromNow
    val sem = new Semaphore(config.concurrent_reqs)

    // log.println(s"# starting experiments")

    val keys = mutable.ArrayBuffer[Int](0 to nsets :_*)
    val maxkey = new AtomicInteger(nsets + 1)

    while (deadline.hasTimeLeft &&
      sem.tryAcquire(deadline.timeLeft.inMillis, TimeUnit.MILLISECONDS)) {
      val i = dist.sample()
      val k = keys(i)
      val key = k.id
      val handle = counter(key)

      def check(t: Truth) = {
        handle.read().instrument(timerRead) map {
          case v: Interval[Long] =>
            if (v.contains(t.target)) {
              countCorrect += 1
              countContains += 1
            } else {
              countIncorrect += 1
            }
            val width = v.max - v.min
            histIntervalWidth << width
            histIntervalPercent << (width / v.median * 10000).toLong
            // log.println(s"# [$k] truth = ${t.target}, got = $v")

          case r: Inconsistent[Long] =>
            val v = r.get
            if (v == t.target) countCorrect += 1
            else countIncorrect += 1

            val d = Math.abs(t.target - r.get)
            histError << d
            histIntervalPercent << (d.toDouble / v * 10000).toLong
            if (r.get < t.target) countErrorNegative += 1
            // log.println(s"# [$k] truth = ${t.target}, got = $r")

          case e =>
            log.println(s"!! unhandled case: $e")

        } flatMap { _ =>
          // now replace this key with a new one
          val j = maxkey.getAndIncrement()
          init(i, j.id) map { _ => keys(i) = j }
        }
      }

      val f = weightedSample(mix) match {

        case 'incr =>
          val t = truth(key)
          val tval = t.start()
          if (tval > t.target) {
            t.complete()
            Future(())
          } else {
            handle.incr().instrument(timerIncr) flatMap { _ =>
              if (t.complete()) {
                check(t)
              } else {
                Future(())
              }
            }
          }

        case 'read =>
          handle.read().instrument(timerRead).unit

        case op =>
          sys.error(s"Unhandled op: $op")
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
    println(s"# checking eventually correct")

  }

}

object RawMixCounter extends {
  override implicit val space = KeySpace("rawmix")
} with Connector {

  class Truth(
      val target: Long,
      v: AtomicLong = new AtomicLong,
      inflight: AtomicLong = new AtomicLong)
  {
    def start() = {
      inflight.incrementAndGet()
      v.incrementAndGet()
    }
    def complete() = {
      inflight.decrementAndGet() == 0 && v.get >= target
    }
    def get = v.get()
  }

  object Truth {
    val values = new ConcurrentHashMap[UUID, Truth]()

    def init(key: UUID, target: Long, initial: Long = 0L) = {
      Truth.values.put(key, new Truth(target, new AtomicLong(initial)))
    }
  }

  def truth(key: UUID): Truth = Truth.values.get(key)

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
