package owl

import java.util.concurrent.TimeUnit

import com.codahale.metrics.ConsoleReporter
import com.websudos.phantom.connectors.KeySpace
import org.apache.commons.math3.distribution.ZipfDistribution

import com.websudos.phantom.dsl._
import scala.concurrent.{ExecutionContextExecutorService, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Random
import Util._

import scala.language.postfixOps

trait Retwis extends OwlService {

  override implicit val space = KeySpace("owl_retwis")

  implicit val consistency =
    config.getString("owl.consistency") match {
      case "strong" => ConsistencyLevel.ALL
      case "weak" => ConsistencyLevel.ONE
      case c => throw new RuntimeException(s"invalid consistency in config: $c")
    }

  val zipf = config.getDouble("retwis.zipf")

  val nUsers = config.getInt("retwis.initial.users")
  val avgFollowers = config.getInt("retwis.initial.followers")
  val tweetsPerUser = config.getInt("retwis.initial.tweets")

  val parallelCap = config.getInt("owl.cap")
  val duration = config.getInt("retwis.duration").seconds

  object Zipf {
    val zipfUser = new ZipfDistribution(nUsers, zipf)

    def user() = User.id(zipfUser.sample())
  }

  object Uniform {
    def user() = User.id(Random.nextInt())
  }

  val WORDS = Vector("small batch", "Etsy", "axe", "plaid", "McSweeney's", "VHS", "viral", "cliche", "post-ironic", "health", "goth", "literally", "Austin", "brunch", "authentic", "hella", "street art", "Tumblr", "Blue Bottle", "readymade", "occupy", "irony", "slow-carb", "heirloom", "YOLO", "tofu", "ethical", "tattooed", "vinyl", "artisan", "kale", "selfie")

  def randomText(n: Int = 4): String = {
    (0 to n).map(_ => WORDS.sample).mkString(" ")
  }

  def randomTweet() = Tweet(user = Zipf.user(), body = randomText())

  def initSocialGraph(nUsers: Int, avgFollowers: Int, zipf: Double = 1.0): Future[Unit] = {

    // create users with UUIDs generated from numbers: 1..nUsers
    val users = (1 to nUsers) map { i =>
      service.store(service.randomUser(id = User.id(i)))
    }

    // follows (can run concurrently with users if needed
    val nFollows = nUsers * avgFollowers
    val follows = (1 to nFollows) map { _ =>
      service.follow(Uniform.user(), Zipf.user())
    }

    Seq(users.bundle, follows.bundle).bundle.map(_ => ())
  }

  object Tasks {

    sealed abstract class Task(
      val body: () => Future[Unit]
    ) extends (() => Future[Unit]) {
      def apply = body() map { _ => metric.retwisOps.mark() }
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
        _ <- tweets
            .filter(i => Random.nextDouble() > 0.4)
            .map { t =>
              service.retweet(t.id, user)
            }
            .bundle
      } yield ()
    })

  }


}

object Workload extends Retwis {

  def apply() {
    println(s"# Running workload for $duration, with $parallelCap at a time.")

    import Tasks._
    val mix = Map(
      NewUser -> 0.02,
      Follow -> 0.05,
      Unfollow -> 0.03,
      Tweet -> 0.20,
      Timeline -> 0.70
    )

    // only generate tasks as needed
    implicit val ec = boundedQueueExecutionContext(capacity = parallelCap)

    val deadline = duration.fromNow
    val all =
      Stream from 1 map { i =>
        weightedSample(mix)()
      } takeWhile { _ =>
        deadline.hasTimeLeft
      } bundle

    await(all)

    println("#> Workload complete.")
    println("#### Metrics")
    ConsoleReporter.forRegistry(metricRegistry)
        .convertRatesTo(TimeUnit.SECONDS)
        .build()
        .report()
    println("####")
  }

  def main(args: Array[String]): Unit = {
    apply()
    sys.exit()
  }

}

object Init extends Retwis {

  def apply() {
    println("#> Resetting keyspace.")
    service.resetKeyspace()

    println("#> Creating tables.")
    service.createTables()

    println("#> Initializing social graph.")
    initSocialGraph(nUsers, avgFollowers, zipf).await()

    println("#> Initializing tweets.")
    var tweetsBy1 = false
    val nTweets = tweetsPerUser * nUsers
    val user1id = User.id(1)
    val fTweets = (0 to nTweets) map { _ =>
      val t = randomTweet()
      if (t.user == user1id) tweetsBy1 = true
      service.post(t)
    }
    fTweets.bundle.await()

    val ts = service.followersOf(user1id)
        .flatMap { fs =>
          fs.take(5).map(f => service.timeline(user = f, limit = 1)).bundle
        }
        .map {
          _.flatten
        }
        .await()
        .toSeq

    println(s"[spot-check]\n - ${ts.mkString("\n - ")}".cyan)

    println("#> Init complete.")
  }

  def main(args: Array[String]) {
    apply()
    sys.exit() // because we have extra threads sitting around...
  }

}

object All {
  def main(args: Array[String]) {
    Init()
    Workload()
    sys.exit()
  }
}