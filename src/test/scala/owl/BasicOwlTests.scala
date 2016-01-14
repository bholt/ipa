package owl

import com.datastax.driver.core.ProtocolVersion
import org.scalatest.OptionValues._

import scala.concurrent.Future
import scala.concurrent.duration._
import com.websudos.phantom.dsl._
import com.websudos.phantom.connectors.KeySpace

import scala.language.postfixOps


class BasicOwlTests extends OwlTest {
  override implicit val space = KeySpace("owl_basic")
  implicit val consistency = ConsistencyLevel.ALL

  implicit override val patienceConfig =
    PatienceConfig(timeout = 500.millis, interval = 10.millis)

  "Connector" should "have valid protocol version" in {
    val protocolVersion = implicitly[Session].getCluster.getConfiguration.getProtocolOptions.getProtocolVersion
    println(s"protocol version: $protocolVersion")
    assert(protocolVersion.compareTo(ProtocolVersion.V1) > 0)
  }

  "User table" should "allow inserting and deleting" in {
    val u = service.randomUser()

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

  val arthur = User(username = "tealuver", name = "Arthur Dent")
  val ford = User(username = "hastowel", name = "Ford Prefect")
  val zaphod = User(username = "froodyprez", name = "Zaphod Beeblebrox")

  "Followers table" should "allow following" in {


    val stores = Future.sequence(Vector(arthur, ford, zaphod) map service.store)
    whenReady(stores) { ids =>
      ids shouldBe Vector(arthur.id, ford.id, zaphod.id)
    }
    println("-- created Arthur, Ford & Zaphod")

    val follows = for {
      _ <- service.follow(arthur.id, zaphod.id)
      _ <- service.follow(ford.id, zaphod.id)
      _ <- service.follow(ford.id, arthur.id)
    } yield ()
    follows.isReadyWithin(100 millis)
    println("-- set up follows")
  }

  it should "allow getting followers of a user" in {
    service.followersOf(zaphod.id).futureValue.toSet shouldBe Set(ford.id, arthur.id)
  }

  val tweetTea = Tweet(
    user = arthur.id,
    body = "Nutri-matic dispenser: almost, but not quite, entirely unlike tea. #wtf"
  )
  val tweetEgo = Tweet(
    user = zaphod.id,
    body = "If anything's more important than my ego, I want it caught and shot now."
  )

  "Tweets" should "be posted" in {
    service.post(tweetTea).futureValue shouldBe tweetTea.id
    service.post(tweetEgo).futureValue shouldBe tweetEgo.id
    println("-- tweeted")
  }

  it should "show up on timelines" in {

    whenReady(service.timeline(arthur.id, 10)) { iter =>
      val tweets = iter.toVector
      tweets.length shouldBe 1

      tweets(0) shouldEqual tweetEgo
      tweets(0).user shouldEqual zaphod.id
    }

    whenReady(service.timeline(ford.id, 10)) { iter =>
      val tweets = iter.toVector

      tweets.length shouldBe 2
      tweets should contain (tweetEgo)
      tweets should contain (tweetTea)
    }

  }

  "Followers table" should "support unfollowing" in {
    service.unfollow(arthur.id, zaphod.id).isReadyWithin(100 millis)

    // now Zaphod should only have 1 follower (Ford)
    service.followersOf(zaphod.id).futureValue.toSet shouldBe Set(ford.id)
  }

  "Retweets" should "be counted" in {
    service.retweet(tweetEgo.id, ford.id).futureValue shouldBe Some(tweetEgo.id)

    retweets(tweetEgo.id).size().futureValue shouldBe 1

    service.retweet(tweetEgo.id, arthur.id).futureValue shouldBe Some(tweetEgo.id)

    whenReady(service.getTweet(tweetEgo.id)) { opt =>
      opt.value.retweets shouldBe 2
      opt.value.body shouldBe tweetEgo.body
    }
  }

  it should "not be duplicated" in {
    retweets(tweetEgo.id).size().futureValue shouldBe 2
    service.retweet(tweetEgo.id, ford.id).futureValue shouldBe None
    retweets(tweetEgo.id).size().futureValue shouldBe 2
  }
}
