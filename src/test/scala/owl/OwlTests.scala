package owl

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Inspectors, Matchers, FlatSpec}
import org.scalatest.OptionValues._

abstract class OwlSpec extends FlatSpec with Matchers with Inspectors with ScalaFutures

class OwlTest extends OwlSpec with CasperService with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    service.createTables
  }

  override def afterAll(): Unit = {
//     service.cleanupTables
  }

  "User table" should "allow inserting and deleting" in {
    val u = service.randomUser

    whenReady(service.store(u)) { result =>
      result.isExhausted shouldBe true
      result.wasApplied shouldBe true
    }
    println(s"-- stored ${u.name}")

    whenReady(service.getUserById(u.id)) { r =>
      // implies that the option has Some, and checks its fields
      r.value should have (
        'username (u.username),
        'name (u.name),
        'created (u.created)
      )
    }
    println(s"-- verified ${u.name} exists")

    whenReady(service.delete(u)) { r =>
      r.isExhausted shouldBe true
      r.wasApplied shouldBe true
    }
    println(s"-- deleted ${u.name}")

    whenReady(service.getUserById(u.id)) { r =>
      r shouldBe None
    }
    println(s"-- verified ${u.name} was deleted")

  }

}
