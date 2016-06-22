package ipa

import com.twitter.util.{Await => TwAwait, Future => TwFuture}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration.Duration

trait OwlSpecCommon {
  import Util.durationScalaToTwitter
  implicit class TwFutureValue[T](f: TwFuture[T]) {
    def futureValue(implicit timeout: Duration): T = TwAwait.result(f, timeout)
  }
}


trait OwlSpec extends FlatSpec with Matchers with Inspectors with ScalaFutures with OwlSpecCommon

trait OwlWordSpec extends WordSpec with Matchers with Inspectors with ScalaFutures with OptionValues with TryValues with OwlSpecCommon

trait OwlFreeSpec extends FreeSpec with Matchers with Inspectors with ScalaFutures with OptionValues with TryValues with OwlSpecCommon

trait OwlTest extends OwlSpec with IPAService with BeforeAndAfterAll  {

  override def beforeAll(): Unit = {
    super.beforeAll()
    service.resetKeyspace()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

}
