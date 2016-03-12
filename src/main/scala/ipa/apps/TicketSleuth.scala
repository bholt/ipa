package ipa.apps

import java.util.concurrent.Semaphore

import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import ipa.{BoundedCounter, IPACounter}
import org.apache.commons.math3.distribution.{NormalDistribution, ZipfDistribution}
import owl.Consistency._
import owl.RawMixCounter._
import owl.{Connector, Consistency, OwlService, _}
import owl.Util._

import scala.concurrent.duration._
import scala.util.Random

class TicketSleuth(val duration: FiniteDuration) extends {
  override implicit val space = KeySpace("tickets")
} with OwlService {
  import Consistency._

  object m {
    val takeLatency = metrics.create.timer("take_latency")
    val readLatency = metrics.create.timer("read_latency")

    val take_success = metrics.create.counter("take_success")
    val take_failure = metrics.create.counter("take_failure")

    val remaining = metrics.create.histogram("remaining")
  }

  val nevents = config.tickets.initial.events

  val ntickets = {
    val mean = config.tickets.initial.remaining
    new NormalDistribution(mean, mean / 4)
  }

  val zipfDist = new ZipfDistribution(nevents, config.zipf)

  def zipfID() = id(zipfDist.sample())
  def urandID() = id(Random.nextInt(nevents))

  val tickets = new BoundedCounter("tickets")

  def generate(): Unit = {
    tickets.create().await()
    tickets.truncate().await()

    0 to nevents map { i => tickets(i.id).init(ntickets.sample().toInt) }
  }

  val mix = Map(
    'take -> 0.4,
    'read -> 0.6
  )

  def run() {

    val actualDurationStart = Deadline.now
    val deadline = duration.fromNow
    val sem = new Semaphore(config.concurrent_reqs)

    while (deadline.hasTimeLeft) {
      sem.acquire()
      val tick = tickets(zipfID())
      val op = weightedSample(mix)
      val f = op match {
        case 'take =>
          for (taken <- tick.decr(1).instrument(m.takeLatency)) {
            if (taken) m.take_success += 1 else m.take_failure += 1
          }
        case 'read =>
          for (remaining <- tick.value().instrument(m.readLatency)) {
            m.remaining << remaining
          }
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

object TicketSleuth extends {
  override implicit val space = KeySpace("tickets")
} with Connector {

  def main(args: Array[String]): Unit = {
    if (config.do_reset) dropKeyspace()
    createKeyspace()

    val warmup = new TicketSleuth(5 seconds)
    println(s">>> generating (${warmup.nevents}")
    warmup.generate()

    println(s">>> warmup (${warmup.duration})")
    warmup.run()

    reservations.clients.values.map(_.metricsReset()).bundle().await()

    val workload = new TicketSleuth(config.duration)
    println(s">>> workload (${workload.duration})")
    workload.run()
    workload.metrics.dump()

    sys.exit()
  }

}
