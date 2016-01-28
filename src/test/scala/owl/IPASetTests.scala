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

  override implicit val space = KeySpace("ipa_set_tests")

  override def beforeAll() = createKeyspace(session)

  val u1 = id(1)
  val u2 = id(2)
  val u3 = id(3)

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

    "get strong consistency" in {
      val t = now()
      whenReady( s(u1).size() ) { r =>
        t.elapsed should be < (s.latencyBound/2)
        println(s"set(u1).size => $r")
        r.get shouldBe 2
        r.consistency shouldBe ConsistencyLevel.ALL
      }
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

    "miss its deadline" in {
      val t = now()
      whenReady( s(u1).size() ) { r =>
        t.elapsed should be > s.latencyBound
        println(s"hasty: u1.size => $r")
        r.consistency shouldBe ConsistencyLevel.ONE
      }
    }

  }

}
