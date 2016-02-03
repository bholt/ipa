package owl

import java.util.concurrent.TimeoutException

import com.datastax.driver.core.ConsistencyLevel
import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import org.apache.commons.math3.distribution.ZipfDistribution
import org.joda.time.format.PeriodFormat
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.collection.JavaConversions._
import Util._

import scala.util.{Try, Random}

class RawMix(val duration: FiniteDuration) extends OwlService {
  override implicit val space = RawMix.space

  val nsets = config.rawmix.nsets
  val mix = config.rawmix.mix

  val zipfDist = new ZipfDistribution(nsets, config.zipf)

  def zipfID() = id(zipfDist.sample())
  def urandID() = id(Random.nextInt(nsets))

  val set = new IPAUuidSet("raw") with LatencyBound {
    override val latencyBound = config.bound.latency
  }

  val actualDuration = metrics.timer("actual_duration")

  val timerAdd      = metrics.timer("add_latency")
  val timerContains = metrics.timer("contains_latency")
  val timerSize     = metrics.timer("size_latency")

  val countContainsStrong = metrics.counter("contains_strong")
  val countContainsWeak   = metrics.counter("contains_weak")
  val countSizeStrong     = metrics.counter("size_strong")
  val countSizeWeak       = metrics.counter("size_weak")

  def recordResult[T](op: Symbol, r: Rushed[T]): Rushed[T] = {
    (op, r.consistency) match {
      case ('contains, ConsistencyLevel.ALL) => countContainsStrong += 1
      case ('contains, ConsistencyLevel.ONE) => countContainsWeak += 1
      case ('size,     ConsistencyLevel.ALL) => countSizeStrong += 1
      case ('size,     ConsistencyLevel.ONE) => countSizeWeak += 1
    }
    r
  }

  def instrument[T, U](op: Symbol)(f: Future[T]): Future[T] = {
    op match {
      case 'add      => f.instrument(timerAdd)
      case 'contains => f.instrument(timerContains)
      case 'size     => f.instrument(timerSize)
    }
  }

  def run() {

    set.create().await()

    // only generate tasks as needed, limit parallelism
    implicit val ec = boundedQueueExecutionContext(
      workers = config.nthreads,
      capacity = config.cap
    )

    val durationTimer = actualDuration.timerContext()
    val deadline = duration.fromNow
    val all = Stream from 1 map { i =>
      val handle = set(zipfID())
      val op = weightedSample(mix)
      op match {
        case 'add =>
          handle.add(urandID())
              .instrument(timerAdd)
              .unit
        case 'contains =>
          handle.contains(urandID())
              .instrument(timerContains)
              .map(recordResult(op, _))
              .unit
        case 'size =>
          handle.size()
              .instrument(timerSize)
              .map(recordResult(op, _))
              .unit
      }
    } takeWhile { _ =>
      deadline.hasTimeLeft
    } bundle

    Try(all.await(duration)) recover { case _: TimeoutException => () }
    val tms = durationTimer.stop().nanos.toMillis
    println(s"# Done in ${tms/1000}.${tms%1000}s")
    // ec.shutdownNow()
  }

}

object RawMix extends Connector {

  override implicit val space = KeySpace("rawmix")

  def main(args: Array[String]): Unit = {
    if (config.do_reset) dropKeyspace()
    createKeyspace()

    val warmup = new RawMix(5 seconds)
    println(s">>> warmup (${warmup.duration})")
    warmup.run()

    val workload = new RawMix(config.duration)
    println(s">>> workload (${workload.duration})")
    workload.run()
    workload.dumpMetrics()

    sys.exit()
  }

}
