package ipa

import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import owl.{Connector, OwlService, OwlWordSpec}
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

    "be created" in {
      s.create().await()
    }

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
}
