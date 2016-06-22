package ipa

import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Inspectors, Matchers, WordSpec}

import scala.concurrent.duration._
import ipa.Util._
import ipa.adts._
import ipa.types._

import scala.language.postfixOps

class IPASetWithErrorBounds extends {
  override implicit val space = KeySpace("setwitherror")
} with WordSpec with IPAService with BeforeAndAfterAll
    with Matchers with Inspectors with ScalaFutures {

  def now() = Deadline.now

  val timeout = 2 seconds
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = timeout, interval = 20 millis)

  println(s"create keyspace ${space.name} in beforeAll")
  if (config.do_reset) dropKeyspace()
  createKeyspace(session)

  val u1 = id(1)
  val u2 = id(2)
  val u3 = id(3)

  def test_generic(s: IPASet[UUID]): Unit = {

    "be created" in {
      s.create().await()
    }

    "be truncated" in {
      s.truncate().await()
    }

    "add items" in {
      s(u1).add(u2).await(timeout)
      s(u1).add(u3).await(timeout)
    }

    "contain added items" in {
      Console.err.println(s"${s.name}: checking contains")
      assert(s(u1).contains(u2).futureValue.get)
      assert(s(u1).contains(u3).futureValue.get)
    }

    "have the correct size" in {
      Console.err.println(s"${s.name}: checking size")
      val v: Inconsistent[Long] = s(u1).size().futureValue
      v.get shouldBe 2
    }

    "remove items that exist" in {
      Console.err.println(s"${s.name}: testing remove")
      s(u1).remove(u2).await(timeout)
      s(u1).size().futureValue.get shouldBe 1
    }

    "remove items that don't exist" in {
      s(u1).remove(u1).await(timeout)
      s(u1).size().futureValue.get shouldBe 1
    }

    "be empty initially" in {
      s(u2).contains(u1).futureValue.get shouldBe false
      s(u2).size().futureValue.get shouldBe 0
    }

  }

  "IPA Set with ErrorBound" should {
    val s = new IPASet[UUID]("s") with IPASet.ErrorBound[UUID]
      { override val bound = Tolerance(0.01) }

    test_generic(s)

  }

  "dump metrics" in {
    metrics.dump()
  }
}
