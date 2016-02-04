package owl

import com.datastax.driver.core.ConsistencyLevel
import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import org.apache.commons.math3.distribution.ZipfDistribution
import scala.concurrent._
import scala.concurrent.duration._
import Util._
import java.util.concurrent.Semaphore

import scala.util.{Success, Try, Random}

class RawMix(val duration: FiniteDuration) extends OwlService {
  override implicit val space = RawMix.space

  val nsets = config.rawmix.nsets
  val mix = config.rawmix.mix

  val zipfDist = new ZipfDistribution(nsets, config.zipf)

  def zipfID() = id(zipfDist.sample())
  def urandID() = id(Random.nextInt(nsets))

  val set = (config.bound.latency, config.bound.consistency) match {
    case (Some(latency), None) =>
      new IPAUuidSet("raw") with LatencyBound { override val latencyBound = latency }
    case (None, Some(consistency)) =>
      new IPAUuidSet("raw")
          with ConsistencyBound { override val consistencyLevel = consistency }
    case e =>
      sys.error(s"impossible case: $e")
  }

  val timerAdd      = metrics.timer("add_latency")
  val timerContains = metrics.timer("contains_latency")
  val timerSize     = metrics.timer("size_latency")

  val countContainsStrong = metrics.counter("contains_strong")
  val countContainsWeak   = metrics.counter("contains_weak")
  val countSizeStrong     = metrics.counter("size_strong")
  val countSizeWeak       = metrics.counter("size_weak")

  def recordResult[T](op: Symbol, r: Inconsistent[T]): Inconsistent[T] = {
    val cons = set match {
      case lbound: LatencyBound => r.asInstanceOf[Rushed[T]].consistency
      case cbound: ConsistencyBound => cbound.consistencyLevel
    }

    (op, cons) match {
      case ('contains, ConsistencyLevel.ALL) => countContainsStrong += 1
      case ('contains, ConsistencyLevel.ONE) => countContainsWeak += 1
      case ('size, ConsistencyLevel.ALL) => countSizeStrong += 1
      case ('size, ConsistencyLevel.ONE) => countSizeWeak += 1
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

    val actualDurationStart = Deadline.now
    val deadline = duration.fromNow
    val sem = new Semaphore(config.concurrent_reqs)

    while (deadline.hasTimeLeft) {
      sem.acquire()
      val handle = set(zipfID())
      val op = weightedSample(mix)
      val f = op match {
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
      f onComplete { _ =>
        sem.release()
      }
    }

    val actualTime = actualDurationStart.elapsed
    output += ("actual.time" -> actualTime)
    println(s"# Done in ${actualTime.toSeconds}.${actualTime.toMillis%1000}s")
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
