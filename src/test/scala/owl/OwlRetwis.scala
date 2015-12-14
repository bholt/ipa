package owl

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

import com.websudos.phantom.dsl._
import com.websudos.phantom.connectors.KeySpace
import Util._

class OwlRetwis extends OwlTest {
  override implicit val space = KeySpace("owl_retwis")

  val nUsers = config.getInt("retwis.initial.users")
  val avgFollowers = config.getInt("retwis.initial.followers")
  val tweetsPerUser = config.getInt("retwis.initial.tweets")

  val zipf = config.getDouble("retwis.zipf")

  "OwlRetwis" should "initialize social graph" in {
    Await.result(service.initSocialGraph(nUsers, avgFollowers, zipf), Duration.Inf)
    println(s"-- social graph initialized ($nUsers users, ${nUsers*avgFollowers} follows)")

    // spot checks
    val u1 = service.userUUID(1)
    service.getUserById(u1).futureValue shouldBe defined
    val u1nfollowers = service.followersOf(u1).futureValue.length
    println(s"-- user[1] num followers = $u1nfollowers")
    u1nfollowers should be > avgFollowers/2
  }

  val WORDS = Vector("small batch", "Etsy", "axe", "plaid", "McSweeney's", "VHS", "viral", "cliche", "post-ironic", "health", "goth", "literally", "Austin", "brunch", "authentic", "hella", "street art", "Tumblr", "Blue Bottle", "readymade", "occupy", "irony", "slow-carb", "heirloom", "YOLO", "tofu", "ethical", "tattooed", "vinyl", "artisan", "kale", "selfie")

  def randomText(n: Int = 4): String = {
    (0 to n).map(_ => WORDS.sample).mkString(" ")
  }

  it should "initialize tweets" in {
    val nTweets = tweetsPerUser * nUsers
    val fTweets = (0 to nTweets) map { _ =>
      val user = service.userUUID(Random.nextInt(nUsers))
      service.post(Tweet(user = user, body = randomText()))
    }
    Await.result(Future.sequence(fTweets), 1 minute)
    val u1 = service.userUUID(1)
    val ts = service.timeline(user = u1, limit = 1).futureValue.toVector
    println(s"-- tweets:\n${ts.mkString("\n")}\n----")
    ts.length shouldBe 1
  }

}
