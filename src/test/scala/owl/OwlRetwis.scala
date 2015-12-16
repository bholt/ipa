package owl

import com.datastax.driver.core.ConsistencyLevel
import org.apache.commons.math3.distribution.ZipfDistribution

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

import com.websudos.phantom.dsl._
import com.websudos.phantom.connectors.KeySpace
import Util._

class OwlRetwis extends OwlTest {
  override implicit val space = KeySpace("owl_retwis")
  implicit val consistency = ConsistencyLevel.ALL

  val nUsers = config.getInt("retwis.initial.users")
  val avgFollowers = config.getInt("retwis.initial.followers")
  val tweetsPerUser = config.getInt("retwis.initial.tweets")

  val zipf = config.getDouble("retwis.zipf")

  object Zipf {
    val zipfUser = new ZipfDistribution(nUsers, zipf)
    def user() = User.id(zipfUser.sample())
  }

  object Uniform {
    def user() = User.id(Random.nextInt())
  }

  def initSocialGraph(nUsers: Int, avgFollowers: Int, zipf: Double = 1.0)(implicit consistency: ConsistencyLevel): Future[Unit] = {
    val rnd = new ZipfDistribution(nUsers, zipf)

    // create users with UUIDs generated from numbers: 1..nUsers
    val users = (1 to nUsers) map { i =>
      service.store(service.randomUser(id = User.id(i)))
    }

    // follows (can run concurrently with users if needed
    val nFollows = nUsers * avgFollowers
    val follows = (1 to nFollows) map { _ =>
      service.follow(Uniform.user(), Zipf.user())
    }

    Future.sequence(Seq(
      Future.sequence(users),
      Future.sequence(follows)
    )).map(_ => ())
  }


  "OwlRetwis" should "initialize social graph" in {
    Await.result(initSocialGraph(nUsers, avgFollowers, zipf), Duration.Inf)
    println(s"-- social graph initialized ($nUsers users, ${nUsers*avgFollowers} follows)")

    // spot checks
    val u1 = User.id(1)
    service.getUserById(u1).futureValue shouldBe defined
    val u1nfollowers = service.followersOf(u1).futureValue.length
    println(s"-- user[1] num followers = $u1nfollowers")
    u1nfollowers should be > avgFollowers/2
  }

  val WORDS = Vector("small batch", "Etsy", "axe", "plaid", "McSweeney's", "VHS", "viral", "cliche", "post-ironic", "health", "goth", "literally", "Austin", "brunch", "authentic", "hella", "street art", "Tumblr", "Blue Bottle", "readymade", "occupy", "irony", "slow-carb", "heirloom", "YOLO", "tofu", "ethical", "tattooed", "vinyl", "artisan", "kale", "selfie")

  def randomText(n: Int = 4): String = {
    (0 to n).map(_ => WORDS.sample).mkString(" ")
  }

  def randomTweet() = Tweet(user = Zipf.user(), body = randomText())

  it should "initialize tweets" in {
    var tweetsBy1 = false
    val nTweets = tweetsPerUser * nUsers
    val user1id = User.id(1)
    val fTweets = (0 to nTweets) map { _ =>
      val t = randomTweet()
      if (t.user == user1id) tweetsBy1 = true
      service.post(t)
    }
    Await.result(Future.sequence(fTweets), 1 minute)

    tweetsBy1 shouldBe true
    val ts = service.followersOf(user1id)
        .flatMap { fs =>
          fs.take(5).map(f => service.timeline(user = f, limit = 1)).bundle
        }
        .map { _.flatten }
        .futureValue
        .toSeq

    println(s"Tweets:\n- ${ts.mkString("\n- ")}")
    ts.length should be >= 1
    ts.toSet.size should be (1)
  }

  object Mix extends Enumeration {
    val NewUser, Follow, Unfollow, Tweet, Retweet, Timeline = Value
  }

  object Workload {

    sealed abstract class Task(
      val body: () => Future[Unit]
    ) extends (() => Future[Unit]) {
      def apply = body()
    }



    case object NewUser extends Task(() =>
      service.store(service.randomUser()) map { _ => () }
    )

    case object Follow extends Task(() =>
      service.follow(Uniform.user(), Zipf.user())
    )

    case object Unfollow extends Task(() => {
      val followee = Uniform.user()
      for {
        fs <- service.followersOf(followee, limit = 1)
        _ <- fs.map(follower => service.unfollow(follower, followee)).bundle
      } yield ()
    })

    case object Tweet extends Task(() => {
      service.post(randomTweet()) map { _ => () }
    })

    case object Timeline extends Task(() => {
      val user = Uniform.user()
      for {
        tweets <- service.timeline(user, limit = 10)
        _ <- {
          tweets.filter(i => Random.nextDouble() > 0.4)
              .map { t =>
                service.retweet(t.id, user)
              }
              .bundle
        }
      } yield ()
    })
  }

  it should "run each work once" in {
    import Workload._
    println("-- NewUser")
    NewUser()

    println("-- Follow")
    Follow()

    println("-- Unfollow")
    Unfollow()

    println("-- Tweet")
    Tweet()

    println("-- Timeline")
    Timeline()

  }

  it should "run workload" in {
    import Workload._

    val mix = Map(
      NewUser -> 0.02,
      Follow -> 0.05,
      Unfollow -> 0.03,
      Tweet -> 0.20,
      Timeline -> 0.70
    )

    val fut = weightedSample(mix)()
    await(fut)
  }
}
