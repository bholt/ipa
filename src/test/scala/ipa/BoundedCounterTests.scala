package ipa

import java.util.concurrent.TimeUnit

import com.websudos.phantom.connectors.KeySpace
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._
import owl._
import owl.Util._
import com.twitter.util.{Duration => TwDuration, Future => TwFuture}

import scala.concurrent.duration._

class BoundedCounterTests extends {
  override implicit val space = KeySpace("bc_tests")
} with OwlFreeSpec with OwlService with BeforeAndAfterAll {
  import Console.err
  def now() = Deadline.now

  implicit val timeout = 2 seconds
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = timeout, interval = 20 millis)

  if (config.do_reset) dropKeyspace()
  createKeyspace()

  import owl.Conversions._

  // help inspect returned metrics
  implicit class Lookup(a: Any) {
    def lookup(s: String) = a.asInstanceOf[Map[String,Any]](s)
  }

  import BoundedCounter._

  "BoundedCounter with StrongBounds" - {
    val bc = new BoundedCounter("bc") with StrongBounds
    val c1 = bc(1.id)

    "be created" in {
      bc.create().await()
    }

    "be truncated" in {
      bc.truncate().await()
    }

    "init a new counter with min of 0" in {
      c1.init(0).futureValue
    }

    "increment counter" in {
      c1.incr(1).futureValue
      c1.incr(2).futureValue
    }

    "have correct value" in {
      val v: Consistent[Int] = c1.value().futureValue
      assert((v: Int) == 3)
    }

    "have rights to decrement" in {
      val outcomes = (0 until 3).map(_ => c1.decr(1)).bundle().futureValue
      assert(outcomes.forall(_.get), "got: " + outcomes.mkString(", "))
    }

    "have insufficient rights to decrement again" in {
      assert((c1.value().futureValue: Int) == 0)
      assert(!c1.decr(1).futureValue)
    }

    "Performance checks" - {
      "consume locally" in {
        err.println("<consume locally>")
        reservations.resetMetrics()

        err.println("initializing a bunch")
        (2 to 10).map(i => bc(i.id).init(0)).bundle().await()
        err.println("incrementing a bunch")
        (1 to 10).map(i => bc(i.id).incr(100)).bundle().await()

        err.println("doing decrements")
        val results = {
          for (i <- 1 to 10; j <- 1 to 20) yield bc(i.id).decr()
        }.bundle().await()
        assert(results.forall(_.get))

        val m = reservations.getMetrics()
        val forwards_count = m("counters").lookup("forwards").lookup("count") match {
          case l: List[_] => l.asInstanceOf[List[Int]].sum
          case i: Int => i
        }
        forwards_count shouldBe 0
        err.println("</consume locally>")
      }
    }

    "Safety checks" - {
      err.println("<decr check>")
      "never allow more decrements than increments" - {

        "initialize" in {
          err.println("re-initializing")
          Thread.sleep(config.lease.periodMillis)
          reservations.resetMetrics()
          bc.truncate().await()

          val checks = (0 until 10)
              .map { i =>
                val c = bc(i.id)
                for {
                  _ <- c.init(0)
                  v <- c.value()
                } yield v.get == 0
              }
              .bundle()
              .futureValue(Duration.Inf)

          checks.forall(identity) shouldBe true
        }

        "check single" in {
          err.println("check single")
          val checks = {
            for (i <- 0 until 10; j <- 0 until 1) yield {
              val c = bc(i.id)
              for {
                a <- c.decr(1)
              } yield {
                a.get
              }
            }
          }.bundle().await()

          checks should have size 10 * 1
          err.println(checks)
          for (chk <- checks) chk shouldBe false //(false, true)
        }

        "check decr->incr->decr once" in {
          err.println("check once")
          val checks = {
            for (i <- 0 until 10; j <- 0 until 1) yield {
              val c = bc(i.id)
              for {
                a <- c.decr(1)
                _ <- c.incr(1)
                b <- c.decr(1)
              } yield {
                (a.get, b.get)
              }
            }
          }.bundle().await()

          checks should have size 10 * 1
          err.println(checks)
          for (chk <- checks) chk shouldBe(false, true)
        }

        "check decr->incr->decr repeated" in {
          err.println("<check repeated>")
          val iters = 20
          val checks = {
            for (i <- 0 until 10; j <- 0 until iters) yield {
              val c = bc(i.id)
              for {
                a <- c.decr(1)
                _ <- c.incr(1)
                b <- c.decr(1)
              } yield {
                (a.get, b.get)
              }
            }
          }.bundle().await()

          checks should have size 10 * iters
          err.println(checks)

          def count(c: Boolean) = {
            if (c) 1 else 0
          }
          val total = checks map { case (b1, b2) => count(b1) + count(b2) } sum;
          assert(total == 10 * iters)
          err.println("</check repeated>")
        }
      }

      err.println("</decr check>")
    }
  }


  "BoundedCounter with ErrorBounds" - {

    val error = Tolerance(0.1)

    val bc = new BoundedCounter("bc_ebound")
        with ErrorBound { override val bound = error }

    val c1 = bc(1.id)

    "create and truncate" in {
      bc.create().await()
      bc.truncate().await()
    }

    val n = 100

    "incr" in {
      c1.incr(n).futureValue
    }

    "interval value" in {
      val i: Interval[Int] = c1.value().futureValue
      val d = error.delta(n)
      assert(i contains n)
      i.min shouldBe n - d
      i.max shouldBe n + d
    }

    "decr" in {
      val success: Inconsistent[Boolean] = c1.decr(1).futureValue
      assert(success.get)
    }

  }

  "dump metrics" in {
    metrics.dump()
  }
}
