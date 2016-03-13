package ipa

import java.util.UUID

import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._
import owl.{Inconsistent, OwlService, OwlWordSpec}
import owl.Util._

import scala.concurrent.duration._


class IPAPoolTests extends {
  override implicit val space = KeySpace("pool_tests")
} with OwlWordSpec with OwlService {

  import Console.err

  def now() = Deadline.now

  implicit val timeout = 2 seconds
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = timeout, interval = 20 millis)

  println(s"create keyspace ${space.name} in beforeAll")
  if (config.do_reset) dropKeyspace()
  createKeyspace(session)

  val cap = 100

  def test_generic(pools: IPAPool): Unit = {

    val p1 = pools(1.id)

    "be created" in {
      pools.create().await()
    }

    "be truncated" in {
      pools.truncate().await()
    }

    "be initialized" in {
      p1.init(3).futureValue
    }

    "take 2 unique ids" in {
      val i1 = p1.take().futureValue.get
      val i2 = p1.take().futureValue.get
      assert(i1 != i2)
      (i1.toSet intersect i2.toSet) shouldBe empty
    }

    "have 1 remaining" in {
      val v: Inconsistent[Int] = p1.remaining().futureValue
      v.get shouldBe 1
    }

    "take last one" in {
      p1.take().futureValue.get should have size 1
    }

    "be empty" in {
      assert(p1.remaining().futureValue.get == 0L)
      p1.take().futureValue.get shouldBe empty
    }
  }

  "IPAPool with StrongBounds" should {

    val pools = new IPAPool("strong") with IPAPool.StrongBounds

    test_generic(pools)

  }

  "IPAPool with WeakBounds" should {

    val pools = new IPAPool("weak") with IPAPool.WeakBounds

    test_generic(pools)

  }
}
