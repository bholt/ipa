package owl

import org.scalactic.Equality
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._

trait OwlSpec extends FlatSpec with Matchers with Inspectors with ScalaFutures

trait OwlWordSpec extends WordSpec with Matchers with Inspectors with ScalaFutures

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
