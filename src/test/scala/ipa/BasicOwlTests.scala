package ipa

import java.util.UUID

import com.datastax.driver.core.ProtocolVersion
import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import ipa.Util._
import ipa.adts._
import ipa.apps.retwis.Retwis
import org.scalatest.OptionValues._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps


class BasicOwlTests extends {
  override implicit val space = KeySpace("owl_basic")
} with OwlTest with Retwis {

  override implicit val consistency = ConsistencyLevel.ALL

  implicit override val patienceConfig =
    PatienceConfig(timeout = 500.millis, interval = 10.millis)

  "Connector" should "have valid protocol version" in {
    val protocolVersion = implicitly[Session].getCluster.getConfiguration.getProtocolOptions.getProtocolVersion
    println(s"protocol version: $protocolVersion")
    assert(protocolVersion.compareTo(ProtocolVersion.V1) > 0)
  }

  "Tables" should "be created" in {
    createTables()
  }

  "User table" should "allow inserting and deleting" in {
    val u = randomUser()

    whenReady(store(u)) { id =>
      id shouldBe u.id
    }
    println(s"-- stored ${u.name}")

    whenReady(getUserById(u.id)) { r =>
      // implies that the option has Some, and checks its fields
      r.value should have (
        'username (u.username),
        'name (u.name),
        'created (u.created)
      )
    }
    println(s"-- verified ${u.name} exists")

    whenReady(delete(u)) { r =>
      r.isExhausted shouldBe true
      r.wasApplied shouldBe true
    }
    println(s"-- deleted ${u.name}")

    whenReady(getUserById(u.id)) { r =>
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


    val stores = Future.sequence(Vector(arthur, ford, zaphod) map store)
    whenReady(stores) { ids =>
      ids shouldBe Vector(arthur.id, ford.id, zaphod.id)
    }
    println("-- created Arthur, Ford & Zaphod")

    val follows = for {
      _ <- follow(arthur.id, zaphod.id)
      _ <- follow(ford.id, zaphod.id)
      _ <- follow(ford.id, arthur.id)
    } yield ()
    assert(follows.isReadyWithin(2 seconds))
    println("-- set up follows")
  }

  it should "allow getting followers of a user" in {
    followersOf(zaphod.id).futureValue.toSet shouldBe Set(ford.id, arthur.id)
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
    post(tweetTea).futureValue shouldBe tweetTea.id
    post(tweetEgo).futureValue shouldBe tweetEgo.id
    println("-- tweeted")
  }

  it should "show up on timelines" in {
//    val weak = ConsistencyLevel.ONE
//    val strong = ConsistencyLevel.ALL
//
//    val tStrong: Iterator[Tweet] = service.timeline2(strong)(arthur.id, 10).await
//
//    val tWeak: Inconsistent[Iterator[Tweet]] = service.timeline2(weak)(arthur.id, 10).await
//
//    val tStale: Stale[Iterator[Tweet]] = service.timeline2(100 millis)(arthur.id, 10).await

    whenReady(timeline(arthur.id, 10)) { iter =>
      val tweets = iter.toVector
      tweets.length shouldBe 1

//      tweets(0) shouldEqual tweetEgo
      tweets(0).user shouldEqual zaphod.id
    }

    whenReady(timeline(ford.id, 10)) { iter =>
      val tweets = iter.toVector

      tweets.length shouldBe 2
      tweets should contain (tweetEgo)
      tweets should contain (tweetTea)
    }

  }

  "Followers table" should "support unfollowing" in {
    unfollow(arthur.id, zaphod.id).isReadyWithin(100 millis)

    // now Zaphod should only have 1 follower (Ford)
    followersOf(zaphod.id).futureValue.toSet shouldBe Set(ford.id)
  }

  "Retweets" should "be counted" in {
    whenReady(retweet(tweetEgo, ford.id)) { _ =>
      retweets(tweetEgo.id).size().futureValue shouldBe 1
    }

    whenReady(retweet(tweetEgo, arthur.id)) { _ =>
      retweets(tweetEgo.id).size().futureValue shouldBe 2
    }

    whenReady(getTweet(tweetEgo.id)) { opt =>
      opt.value.retweets shouldBe 2
      opt.value.body shouldBe tweetEgo.body
    }
  }

  it should "contain retweeters" in {
    retweets(tweetEgo.id).contains(arthur.id).futureValue shouldBe true
    retweets(tweetEgo.id).contains(ford.id).futureValue shouldBe true
  }

  it should "not be duplicated" in {
    retweets(tweetEgo.id).size().futureValue shouldBe 2
    whenReady(retweet(tweetEgo, ford.id)) { _ =>
      retweets(tweetEgo.id).size().futureValue shouldBe 2
    }
  }

  val sPlain = new IPASetImplPlain[UUID, UUID]("splain", config.consistency)
  val sCounter = new IPASetImplWithCounter[UUID, UUID]("scounter", config.consistency)
  val sCollect = new IPASetImplCollection[UUID, UUID]("scollect", config.consistency)

  val sets = Seq(sPlain, sCounter, sCollect)

  "IPASet" should "create tables" in {
    sets.map(_.create()).bundle.await()
  }

  val u1 = User.id(1)
  val u2 = User.id(2)

  it should "support add" in {
    sets.map(_.add(u1, u2)).bundle.await()
  }

  it should "support contains" in {
    for (s <- sets) {
      s.contains(u1, u2).futureValue shouldBe true
    }
  }
}
