package ipa

import java.util.UUID

import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._
import owl.{Inconsistent, OwlService}
import owl.Util._

import scala.concurrent.duration._


class IPAPoolTests extends {
  override implicit val space = KeySpace("pool_tests")
} with WordSpec with OwlService with BeforeAndAfterAll
    with Matchers with Inspectors with ScalaFutures with OptionValues {

  def now() = Deadline.now

  val timeout = 2 seconds
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = timeout, interval = 20 millis)

  println(s"create keyspace ${space.name} in beforeAll")
  if (config.do_reset) dropKeyspace()
  createKeyspace(session)

  val cap = 100

  def test_generic(pools: IPAPool): Unit = {

    "be created" in {
      pools.create().await()
    }

    "be truncated" in {
      pools.truncate().await(timeout)
    }

    val p1 = pools(1.id).init(3L).await(timeout)

    "take 2 unique ids" in {
      val i1 = p1.take().futureValue.get
      val i2 = p1.take().futureValue.get
      assert(i1.value != i2.value)
    }

    "have 1 remaining" in {
      val v: Inconsistent[Long] = p1.remaining().futureValue
      v.get shouldBe 1
    }

    "take last one" in {
      p1.take().futureValue.get shouldBe defined
    }

    "be empty" in {
      assert(p1.remaining().futureValue.get == 0L)
      p1.take().futureValue.get shouldBe empty
    }
  }

  "IPA Set with Weak ops" should {

    val pools = new IPAPool("pool_strong") with IPAPool.StrongOps

    test_generic(pools)

  }
}
