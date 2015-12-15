package owl

import org.scalactic.Equality
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Inspectors, Matchers, FlatSpec}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

abstract class OwlSpec extends FlatSpec with Matchers with Inspectors with ScalaFutures

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
    Await.result(service.createTables(), Duration.Inf)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    service.cleanupTables()
  }

}
