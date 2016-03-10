package ipa

import java.util.concurrent.TimeUnit

import com.websudos.phantom.connectors.KeySpace
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._
import owl.OwlService
import owl.Util._
import com.twitter.util.{Duration => TwDuration, Future => TwFuture}

import scala.concurrent.duration._

class BoundedCounterTests extends {
  override implicit val space = KeySpace("bc_tests")
} with WordSpec with OwlService with BeforeAndAfterAll
    with Matchers with Inspectors with ScalaFutures with OptionValues {

  def now() = Deadline.now

  val twtime = TwDuration(2, TimeUnit.SECONDS)
  val timeout = 2 seconds
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = timeout, interval = 20 millis)


  implicit class TwFutureValue[T](f: TwFuture[T]) {
    def futureValue: T = f.await(twtime)
  }

  if (config.do_reset) dropKeyspace()
  createKeyspace()

  val bc = new BoundedCounter("bc")

  "be created" in {
    bc.create().await()
  }

  "be truncated" in {
    bc.truncate().await()
  }

  val c1 = bc(1.id)

  "init a new counter with min = 0" in {
    c1.init(0)
  }

  "increment counter" in {
    c1.incr(1).futureValue
    c1.incr(2).futureValue
  }

  "have correct value" in {
    assert(c1.value().futureValue == 3)
  }

  "have rights to decrement" in {
    TwFuture.join(
      c1.decr(1),
      c1.decr(1),
      c1.decr(1)
    ).await()
  }

  "have insufficient rights to decrement again" in {
    c1.decr(1).await()
  }

}
