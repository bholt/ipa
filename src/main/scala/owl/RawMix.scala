package owl

import java.util.UUID

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

  import Consistency._

  val set = config.bound match {

    case Consistency(Weak, _) =>
      new IPAUuidSet("raw")
          with ConsistencyBound { override val consistencyLevel = Weak }

    case Consistency(Strong, _) =>
      new IPAUuidSet("raw")
          with ConsistencyBound { override val consistencyLevel = Strong }

    case Latency(lat) =>
      new IPAUuidSet("raw") with LatencyBound { override val latencyBound = lat }

    case e =>
      sys.error(s"impossible case: $e")
  }

  val timerAdd      = metrics.create.timer("add_latency")
  val timerContains = metrics.create.timer("contains_latency")
  val timerSize     = metrics.create.timer("size_latency")

  val countContainsStrong = metrics.create.counter("contains_strong")
  val countContainsWeak   = metrics.create.counter("contains_weak")
  val countSizeStrong     = metrics.create.counter("size_strong")
  val countSizeWeak       = metrics.create.counter("size_weak")

  val countConsistent = metrics.create.counter("consistent")
  val countInconsistent = metrics.create.counter("inconsistent")

  def recordResult[T](op: Symbol, r: Inconsistent[T]): Inconsistent[T] = {
    val cons = set match {
      case lbound: LatencyBound => r.asInstanceOf[Rushed[T]].consistency
      case cbound: ConsistencyBound => cbound.consistencyLevel
    }
    (op, cons) match {
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

    val actualDurationStart = Deadline.now
    val deadline = duration.fromNow
    val sem = new Semaphore(config.concurrent_reqs)

    while (deadline.hasTimeLeft) {
      sem.acquire()
      val handle = set(zipfID())
      val op = weightedSample(mix)
      val f = op match {
        case 'add =>
          if (Random.nextDouble() > config.rawmix.check_probability) {
            val v = urandID()
            handle.add(v).instrument(timerAdd)
          } else {
            // get a truly random UUID so it's unlikely to already exist
            // (otherwise our test is bunk
            val v = UUID.randomUUID()
            for {
              add <- handle.add(v).instrument(timerAdd)
              contains <- handle.contains(v).instrument(timerContains)
            } yield {
              recordResult('contains, contains)
              if (contains.get) {
                countConsistent += 1
              } else {
                countInconsistent += 1
              }
              ()
            }
          }

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
    output += ("actual_time" -> actualTime)
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
    workload.metrics.dump()

    sys.exit()
  }

}
