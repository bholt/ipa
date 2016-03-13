package owl

import com.twitter.util.{Future => TwFuture, Await => TwAwait}
import org.scalactic.Equality
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._

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

trait OwlTest extends OwlSpec with OwlService with BeforeAndAfterAll  {

  /** override Tweet equality check to handle the fact when we 'getTweet',
    * we load the user's full name */
  implicit val tweetEquality = new Equality[Tweet] {
    override def areEqual(a: Tweet, x: Any): Boolean = x match {
      case b: Tweet =>
        a.id == b.id &&
        a.body == b.body &&
        a.user == b.user &&
        a.created == b.created &&
        (a.name.isEmpty || b.name.isEmpty || a.name == b.name)
      case _ =>
        false
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    service.resetKeyspace()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

}
