package owl

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Inspectors, Matchers, FlatSpec}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

abstract class OwlSpec extends FlatSpec with Matchers with Inspectors with ScalaFutures

trait OwlTest extends OwlSpec with OwlService with BeforeAndAfterAll  {

  override def beforeAll(): Unit = {
    super.beforeAll()
    Await.result(service.createTables(), Duration.Inf)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    service.cleanupTables()
  }

}
