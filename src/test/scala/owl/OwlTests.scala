package owl

import java.util.UUID

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Inspectors, Matchers, FlatSpec}
import org.scalatest.OptionValues._

import scala.concurrent.{Await,Future}
import scala.concurrent.duration._
import com.websudos.phantom.dsl._

abstract class OwlSpec extends FlatSpec with Matchers with Inspectors with ScalaFutures

class OwlTest extends OwlSpec with OwlService with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    Await.result(service.createTables(), Duration.Inf)
  }

  override def afterAll(): Unit = {
     service.cleanupTables()
  }

  "User table" should "allow inserting and deleting" in {
    val u = service.randomUser

    whenReady(service.store(u)) { id =>
      id shouldBe u.id
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

//  it should "initialize random users & followers" in {
//    Await.ready(service.initUsers(100, 16), 1.minute)
//  }

  "Followers table" should "allow getting followers of a user" in {
    val arthur = User(username = "tealuv", name = "Arthur Dent")
    val ford = User(username = "hastowel", name = "Ford Prefect")
    val zaphod = User(username = "froodyprez", name = "Zaphod Beeblebrox")

    val stores = Future.sequence(Vector(arthur, ford, zaphod) map service.store)
    whenReady(stores) { ids =>
      ids shouldBe Vector(arthur.id, ford.id, zaphod.id)
    }
    println("-- created Arthur, Ford & Zaphod")

    val follows = for {
      _ <- service.follow(arthur.id, zaphod.id)
      _ <- service.follow(ford.id, zaphod.id)
      _ <- service.follow(zaphod.id, ford.id)
    } yield ()
    follows.futureValue shouldBe ()
    println("-- set up follows")

    whenReady(service.followersOf(zaphod.id)) { followers =>
      followers.toSet shouldBe Set(ford.id, arthur.id)
    }

  }

}
