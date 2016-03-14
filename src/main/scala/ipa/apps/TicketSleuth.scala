package ipa.apps

import java.util.concurrent.Semaphore

import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import ipa.{BoundedCounter, IPACounter, IPAPool}
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
  import Console.err
  import Consistency._

  object m {
    val takeLatency = metrics.create.timer("take_latency")
    val readLatency = metrics.create.timer("read_latency")

    val take_success = metrics.create.counter("take_success")
    val take_failed = metrics.create.counter("take_failure")

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

  val tickets = IPAPool.fromNameAndBound("tickets", config.bound)

  def generate(): Unit = {
    tickets.create().await()
    tickets.truncate().await()

    (0 to nevents)
        .map { i => tickets(i.id).init(ntickets.sample().toInt) }
        .bundle()
        .await()
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
      val i = zipfDist.sample()
      val handle = tickets(i.id)
      val op = weightedSample(mix)
      val f = op match {
        case 'take =>
          for (taken <- handle.take(1).instrument(m.takeLatency)) {
            if (taken.get.isEmpty) m.take_failed += 1
            else m.take_success += 1
          }
        case 'read =>
          for (r <- handle.remaining().instrument(m.readLatency)) {
            m.remaining << r.get
          }
      }
      f onSuccess { case _ => sem.release() }
      f onFailure { case e: Throwable =>
        err.println(s"Error with $i (key: ${handle.key}, op: $op)\n  e.getMessage")
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
    println(s">>> generating (${warmup.nevents})")
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
