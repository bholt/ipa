package ipa

import java.util.concurrent.TimeUnit

import com.websudos.phantom.connectors.KeySpace
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._
import owl.{Consistency, Consistent, OwlService, OwlWordSpec}
import owl.Util._
import com.twitter.util.{Duration => TwDuration, Future => TwFuture}

import scala.concurrent.duration._

class BoundedCounterTests extends {
  override implicit val space = KeySpace("bc_tests")
} with OwlWordSpec with OwlService with BeforeAndAfterAll {
  import Console.err
  def now() = Deadline.now

  implicit val timeout = 2 seconds
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = timeout, interval = 20 millis)

  if (config.do_reset) dropKeyspace()
  createKeyspace()

  import owl.Conversions._

  val bc = new BoundedCounter("bc") with BoundedCounter.StrongBounds
  val c1 = bc(1.id)

  "be created" in {
    bc.create().await()
  }

  "be truncated" in {
    bc.truncate().await()
  }

  "init a new counter with min = 0" in {
    c1.init(0).futureValue
  }

  "increment counter" in {
    c1.incr(1).futureValue
    c1.incr(2).futureValue
  }

  "have correct value" in {
    val v: Consistent[Int] = c1.value().futureValue
    assert((v:Int) == 3)
  }

  "have rights to decrement" in {
    val outcomes = (0 until 3).map(_ => c1.decr(1)).bundle().futureValue
    assert(outcomes.forall(_.get), "got: " + outcomes.mkString(", "))
  }

  "have insufficient rights to decrement again" in {
    assert((c1.value().futureValue:Int) == 0)
    assert(!c1.decr(1).futureValue)
  }

  "do a bunch more increments and decrements" in {
    err.println("initializing a bunch")
    (2 to 10).map(i => bc(i.id).init(0)).bundle().await()
    err.println("incrementing a bunch")
    (1 to 10).map(i => bc(i.id).incr(100)).bundle().await()

    err.println("doing decrements")
    val results = {
      for (i <- 1 to 10; j <- 1 to 20) yield bc(i.id).decr()
    }.bundle().await()
    assert(results.forall(_.get))
  }

  "dump metrics" in {
    metrics.dump()
  }
}
