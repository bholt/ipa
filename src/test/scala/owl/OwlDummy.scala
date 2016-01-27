package owl

import scala.concurrent._
import scala.concurrent.duration._
import com.websudos.phantom.dsl._
import scala.language.postfixOps

import Util._

/** Dummy tests (don't run as part of default test suite) */
class OwlDummy extends OwlTest {
  override implicit val space = KeySpace("owl_dummy")

  "Dummy" should "run" in {
    implicit val ec = boundedQueueExecutionContext(capacity = config.cap)

    val duration = 10.seconds
    println(s"# running workload for $duration, with ${config.cap} at a time")
    val deadline = duration.fromNow

    val stream =
      Stream from 1 map { i =>
        println(s"[$i] created")
        Future {
          println(s"[$i] executing")
          blocking { Thread.sleep(1.second.toMillis) }
          println(s"[$i] done")
        }
      } takeWhile { _ =>
        deadline.hasTimeLeft
      } bundle

    await(stream)

    println("####")
  }
}
