package ipa

import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import owl.{Connector, OwlService, OwlWordSpec, Tolerance}
import owl.Util._

import scala.concurrent.duration._

class CounterTests extends {
  override implicit val space = KeySpace(Connector.config.keyspace)
} with OwlWordSpec with OwlService {

  val timeout = 2 seconds
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = timeout, interval = 20 millis)

  if (config.do_reset) dropKeyspace()
  createKeyspace()

  "Counter with WeakOps" should {

    val s = new Counter("weak") with Counter.WeakOps

    "be created" in { s.create().await() }
    "be truncated" in { s.truncate().await() }

    "read default value" in {
      val r = s(0.id).read().futureValue
      r.get shouldBe 0
    }

    "increment" in {
      Seq(
        s(0.id).incr(1),
        s(0.id).incr(2)
      ).bundle.await(timeout)
    }

    "read incremented value" in {
      val r = s(0.id).read().futureValue
      r.get shouldBe 3
    }
  }

  "Counter with StrongOps" should {

    val s = new Counter("strong") with Counter.StrongOps

    "be created" in { s.create().await() }
    "be truncated" in { s.truncate().await() }

    "read default value" in {
      val r = s(0.id).read().futureValue
      r shouldBe 0
    }

    "increment" in {
      Seq(
        s(0.id).incr(1),
        s(0.id).incr(2)
      ).bundle.await(timeout)
    }

    "read incremented value" in {
      val r = s(0.id).read().futureValue
      r shouldBe 3
    }
  }

  "Counter with LatencyBound" should {

    val s = new Counter("latencybound")
        with Counter.LatencyBound { override val bound = 50 millis }

    "be created" in { s.create().await() }
    "be truncated" in { s.truncate().await() }

    "read default value" in {
      val r = s(0.id).read().futureValue
      r.get shouldBe 0
      println(s"read with ${r.consistency}")
    }

    "increment" in {
      Seq(
        s(0.id).incr(1),
        s(0.id).incr(2)
      ).bundle.await(timeout)
    }

    "read incremented value" in {
      val r = s(0.id).read().futureValue
      r.get shouldBe 3
      println(s"read with ${r.consistency}")
    }

  }

  "Counter with ErrorTolerance" should {

    val error = 0.05
    val large = 1000L
    val large_eps = (large * error).toLong

    val s = new Counter("latencybound")
        with Counter.ErrorTolerance { override val tolerance = Tolerance(error) }

    "be created" in { s.create().await() }
    "be truncated" in { s.truncate().await() }

    "read default value" in {
      val r = s(0.id).read().futureValue
      r.min shouldBe 0
      r.max shouldBe 0
    }

    "increment" in {
      Seq(
        s(0.id).incr(1),
        s(0.id).incr(2),
        s(1.id).incr(large)
      ).bundle.await(timeout)
    }

    "read small value" in {
      val r = s(0.id).read().futureValue
      r.min shouldBe 3
      r.max shouldBe 3
    }

    "read large value" in {
      val r = s(1.id).read().futureValue
      r.min should be >= (large - large_eps)
      r.max should be <= (large + large_eps)
    }

    "smaller increments to large value" in {
      val f = for {
        _ <- s(1.id).incr()
        _ <- s(1.id).incr()
        _ <- s(1.id).incr()
        r <- s(1.id).read()
      } yield {
        println(r)
        r.contains(large+3) shouldBe true
        r.max - r.min should be <= 2*large_eps
        r.min should be >= (large - large_eps)
        r.max should be <= (large + 3 + large_eps)
      }
      f.await()
    }

  }


}
