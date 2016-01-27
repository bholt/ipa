package owl

import java.util.concurrent.TimeUnit

import com.codahale.metrics.ConsoleReporter
import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import org.apache.commons.math3.distribution.ZipfDistribution
import owl.Util._

import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.Random

trait Retwis extends OwlService {

  implicit val consistency = config.consistency

  object Zipf {
    val zipfUser = new ZipfDistribution(config.nUsers, config.zipf)

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

    sealed abstract class Task(body: () => Future[Unit])
        extends (() => Future[Unit]) {
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
        f <- followers(followee).random()
        _ <- service.unfollow(f, followee)
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
              service.retweet(t, user)
            }
            .bundle
      } yield ()
    })

  }

  def generate(): Unit = {
    if (!config.do_generate) {
      println("# Skipping data generation.")
      return
    }
    if (!config.do_reset) {
      // if we didn't do reset but are generating data,
      // we should drop existing records

    }

    println(s"# Initializing social graph (${config.nUsers} users, ${config.avgFollowers} avg followers)")
    initSocialGraph(config.nUsers, config.avgFollowers, config.zipf).await()

    println(s"# Initializing tweets (${config.tweetsPerUser} per user)")
    val nTweets = config.tweetsPerUser * config.nUsers
    val fTweets = (0 to nTweets) map { _ => service.post(randomTweet()) }
    fTweets.bundle.await()
    println("# Init complete.")
  }

  def workload(): Unit = {
    println(s"# Running workload for ${config.duration}, with ${config.cap} at a time.")

    import Tasks._
    val mix = Map(
      NewUser -> 0.02,
      Follow -> 0.05,
      Unfollow -> 0.03,
      Tweet -> 0.20,
      Timeline -> 0.70
    )

    // only generate tasks as needed, limit parallelism
    implicit val ec = boundedQueueExecutionContext(
      workers = config.nthreads,
      capacity = config.cap
    )

    val deadline = config.duration.fromNow
    val all =
      Stream from 1 map { i =>
        weightedSample(mix)()
      } takeWhile { _ =>
        deadline.hasTimeLeft
      } bundle

    await(all)

    println("# Workload complete.")
    println("#### Metrics ##################")
    ConsoleReporter.forRegistry(metricRegistry)
        .convertRatesTo(TimeUnit.SECONDS)
        .build()
        .report()

    // dump metrics to stderr (for experiments script to parse)
    if (config.output_json) {
      metric.write(Console.err)
    }
    println("###############################")
  }

}

class RetwisExec extends {
  override implicit val space = KeySpace("owl_retwis")
} with Retwis

object Workload extends RetwisExec {
  override implicit lazy val session = Connector.throttledCluster.connect(space.name)
  def apply() = workload()
  def main(args: Array[String]): Unit = {
    apply()
    sys.exit()
  }
}

object Init extends RetwisExec {
  def apply() = {
    // println(config.toJSON)
    service.resetKeyspace()
    generate()
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