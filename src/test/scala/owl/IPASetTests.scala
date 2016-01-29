package owl

import com.datastax.driver.core.ConsistencyLevel
import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Inspectors, Matchers, BeforeAndAfterAll, WordSpec}

import scala.concurrent.duration._

import Util._

/**
  * Created by bholt on 1/28/16.
  */
class IPASetTests extends WordSpec with OwlService with BeforeAndAfterAll
    with Matchers with Inspectors with ScalaFutures {

  def now() = Deadline.now

  val timeout = 2 seconds
  implicit override val patienceConfig =
    PatienceConfig(timeout = timeout, interval = 20 millis)

  override implicit val space = KeySpace("ipa_set_tests")

  override def beforeAll() = createKeyspace(session)

  val u1 = id(1)
  val u2 = id(2)
  val u3 = id(3)

  "IPASet with LatencyBound" should {

    val s = new IPAUuidSet("snorm")
        with LatencyBound { val latencyBound = 100 millis }

    "be created" in {
      s.create().await()
    }

    "add items" in {
      s(u1).add(u2).await(timeout)
      s(u1).add(u3).await(timeout)
    }

    "contain added items" in {
      assert(s(u1).contains(u2).futureValue.get)
      assert(s(u1).contains(u3).futureValue.get)
    }

    "size reflect added items" in {
      s(u1).size().futureValue.get shouldBe 2
    }

    "remove items that exist" in {
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

  "Quick set" should {

    val s = new IPAUuidSet("suid")
        with LatencyBound { val latencyBound = 5 millis }

    "be created" in {
      println(">>> creating table-based set")
      s.create().await()
    }

    "allow adding" in {
      Seq( s(u1).add(u2), s(u1).add(u3) )
          .bundle
          .await()
    }

    "support rushed size" in {
      whenReady( s(u1).size() ) { r =>
        println(s"set(u1).size => $r")
        r.get shouldBe 2
      }
    }

  }


  "Sloth set" should {

    val s = new IPAUuidSet("sloth")
        with LatencyBound { val latencyBound = 2 seconds }

    "be created" in {
      s.create().await()
    }

    "allow adding" in {
      Seq( s(u1).add(u2), s(u1).add(u3) )
          .bundle
          .await()
    }

    "get strong consistency" ignore {
      val t = now()
      val r = s(u1).size().await(timeout)
      t.elapsed should be < (s.latencyBound/2)
      println(s"set(u1).size => $r")
      r.get shouldBe 2
      r.consistency shouldBe ConsistencyLevel.ALL
    }
  }

  "Hasty set" should {

    val s = new IPAUuidSet("hasty")
        with LatencyBound { val latencyBound = 1 microsecond }

    "be created" in {
      s.create().await()
    }

    "allow adding" in {
      Seq( s(u1).add(u2), s(u1).add(u3) )
          .bundle
          .await()
    }

    "miss its deadline and get weak consistency" ignore {
      val t = now()
      val r = s(u1).size().await(timeout)
      t.elapsed should be > s.latencyBound
      println(s"hasty: u1.size => $r")
      r.consistency shouldBe ConsistencyLevel.ONE
    }

  }

}
