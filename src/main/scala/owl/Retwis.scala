package owl

import java.util.UUID
import java.util.concurrent.{Semaphore, TimeUnit}

import com.codahale.metrics.ConsoleReporter
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.utils.UUIDs
import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl.{DateTime, UUID, _}
import ipa.IPASet
import org.apache.commons.math3.distribution.ZipfDistribution
import org.joda.time.DateTime
import owl.Util._

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.concurrent.duration.Deadline
import scala.language.postfixOps
import scala.util.Random

trait Retwis extends OwlService {

  // User
  case class User(
      id: UUID = UUIDs.random(),
      username: String,
      name: String,
      created: DateTime = DateTime.now()
  )

  object User {
    def id(i: Int) = UUID.nameUUIDFromBytes(BigInt(i).toByteArray)
  }

  class Users extends CassandraTable[Users, User] {
    object id extends UUIDColumn(this) with PartitionKey[UUID]
    object username extends StringColumn(this)
    object name extends StringColumn(this)
    object created extends DateTimeColumn(this)

    override val tableName = "users"
    override def fromRow(r: Row) = User(id(r), username(r), name(r), created(r))
  }

  // Tweets
  case class Tweet(
      id: UUID = UUIDs.timeBased(),
      user: UUID,
      body: String,
      created: DateTime = DateTime.now(),
      retweets: Long = 0,
      name: Option[String] = None
  ) {
    override def toString = {
      s"${name.getOrElse(user)}: $body [$created]"
    }
  }

//  /** override Tweet equality check to handle the fact when we 'getTweet',
//    * we load the user's full name */
//  implicit val tweetEquality = new Equality[Tweet] {
//    override def areEqual(a: Tweet, x: Any): Boolean = x match {
//      case b: Tweet =>
//        a.id == b.id &&
//            a.body == b.body &&
//            a.user == b.user &&
//            a.created == b.created &&
//            (a.name.isEmpty || b.name.isEmpty || a.name == b.name)
//      case _ =>
//        false
//    }
//  }

  /** For displaying tweets (includes user's name, etc). */
  case class DisplayTweet(tweet: Tweet, user: User) {
    override def toString = {
      s"${user.name}: ${tweet.body} [${tweet.created}]"
    }
  }

  class Tweets extends CassandraTable[Tweets, Tweet] {
    object id extends UUIDColumn(this) with PartitionKey[UUID]
    object user extends UUIDColumn(this)
    object body extends StringColumn(this)
    object created extends DateTimeColumn(this)
    override val tableName = "tweets"
    override def fromRow(r: Row) = Tweet(id(r), user(r), body(r), created(r))
  }

  case class TimelineEntry(user: UUID, tweet: UUID, created: DateTime)
  class Timelines extends CassandraTable[Timelines, TimelineEntry] {
    object user extends UUIDColumn(this) with PartitionKey[UUID]
    object tweet extends TimeUUIDColumn(this) with PrimaryKey[UUID] with ClusteringOrder[UUID] with Descending
    object created extends DateTimeColumn(this)
    override val tableName = "timelines"
    override def fromRow(r: Row) = TimelineEntry(user(r), tweet(r), created(r))
  }

  case class Retweet(tweet: UUID, retweeter: UUID)
  class Retweets extends CassandraTable[Retweets, Retweet] {
    object tweet extends UUIDColumn(this) with PartitionKey[UUID]
    object retweeter extends UUIDColumn(this) with PrimaryKey[UUID]
    override val tableName = "retweets"
    override def fromRow(r: Row) = Retweet(tweet(r), retweeter(r))
  }

  case class RetweetCount(tweet: UUID, count: Long)
  class RetweetCounts extends CassandraTable[RetweetCounts, RetweetCount] {
    object tweet extends UUIDColumn(this) with PartitionKey[UUID]
    object count extends CounterColumn(this)
    override val tableName = "retweetCounts"
    override def fromRow(r: Row) = RetweetCount(tweet(r), count(r))
  }

  implicit val consistency = config.consistency

  object Zipf {
    val zipfUser = new ZipfDistribution(config.nUsers, config.zipf)

    def user() = User.id(zipfUser.sample())
  }

  var nusers = config.nUsers

  object Uniform {
    def user() = User.id(Random.nextInt(nusers))
  }

  val WORDS = Vector("small batch", "Etsy", "axe", "plaid", "McSweeney's", "VHS", "viral", "cliche", "post-ironic", "health", "goth", "literally", "Austin", "brunch", "authentic", "hella", "street art", "Tumblr", "Blue Bottle", "readymade", "occupy", "irony", "slow-carb", "heirloom", "YOLO", "tofu", "ethical", "tattooed", "vinyl", "artisan", "kale", "selfie")

  def randomText(n: Int = 4): String = {
    (0 to n).map(_ => WORDS.sample).mkString(" ")
  }

  val FIRST_NAMES = Vector("Arthur", "Ford", "Tricia", "Zaphod")
  val LAST_NAMES = Vector("Dent", "Prefect", "McMillan", "Beeblebrox")

  val users = new Users
  val tweets = new Tweets
  val timelines = new Timelines

  val othersBound = Consistency(config.consistency, config.consistency)

  val retweets = IPASet.fromNameAndBound[UUID]("retweets", config.bound)
  val followers = new IPAUuidSet("followers")
      with ConsistencyBound { override val consistencyLevel = config.consistency }
  val followees = new IPAUuidSet("followees")
      with ConsistencyBound { override val consistencyLevel = config.consistency }

  val tables = List(users, tweets, timelines)

  def randomTweet() = Tweet(user = Zipf.user(), body = randomText())

  def initSocialGraph(nUsers: Int, avgFollowers: Int, zipf: Double = 1.0): Future[Unit] = {

    // create users with UUIDs generated from numbers: 1..nUsers
    val users = (1 to nUsers) map { i =>
      store(randomUser(id = User.id(i)))
    }

    // follows (can run concurrently with users if needed
    val nFollows = nUsers * avgFollowers
    val follows = (1 to nFollows) map { _ =>
      follow(Uniform.user(), Zipf.user())
    }

    Seq(users.bundle, follows.bundle).bundle.map(_ => ())
  }

  def randomUser(
      id: UUID = UUIDs.timeBased(),
      username: String = "",
      name: String = s"${FIRST_NAMES.sample} ${LAST_NAMES.sample}",
      time: DateTime = DateTime.now()
  ): User = {
    val uname = if (username.isEmpty) s"${id.hashCode()}" else username
    User(id, uname, name, time)
  }

  def store(user: User)(implicit consistency: ConsistencyLevel): Future[UUID] = {
    for {
      rs <- users.insert()
          .consistencyLevel_=(consistency)
          .value(_.id, user.id)
          .value(_.username, user.username)
          .value(_.name, user.name)
          .value(_.created, user.created)
          .future()
          .instrument()
    } yield user.id
  }

  def getUserById(id: UUID)(implicit consistency: ConsistencyLevel): Future[Option[User]] = {
    users.select
        .consistencyLevel_=(consistency)
        .where(_.id eqs id)
        .one()
        .instrument()
  }

  def delete(user: User)(implicit consistency: ConsistencyLevel): Future[ResultSet] = {
    users.delete
        .consistencyLevel_=(consistency)
        .where(_.id eqs user.id)
        .future()
        .instrument()
  }

  def follow(follower: UUID, followee: UUID): Future[Unit] = {
    for {
      _ <- followers(followee).add(follower)
      _ <- followees(follower).add(followee)
    } yield ()
  }

  def unfollow(follower: UUID, followee: UUID): Future[Unit] = {
    for {
      _ <- followers(followee).remove(follower)
      _ <- followees(follower).remove(followee)
    } yield ()
  }

  def followersOf(user: UUID): Future[Iterator[UUID]] = {
    followers(user).get().map(_.get.toIterator)
  }

  object prepared {

    lazy val timeline_append: (UUID, UUID, DateTime) => (ConsistencyLevel) => BoundOp[Unit] = {
      val ps = session.prepare(s"INSERT INTO ${space.name}.${timelines.tableName} (${timelines.user.name}, ${timelines.tweet.name}, ${timelines.created.name}) VALUES (?, ?, ?)")
      (user: UUID, tweet: UUID, created: DateTime) =>
        ps.bindWith(user, tweet, created)(_ => ())
    }

    lazy val timeline_get: (UUID, Int) => (ConsistencyLevel) => BoundOp[Iterator[UUID]] = {
      val ps = session.prepare(s"SELECT ${timelines.tweet.name} FROM ${space.name}.${timelines.tableName} WHERE ${timelines.user.name} = ? ORDER BY ${timelines.tweet.name} DESC LIMIT ?")
      (user: UUID, limit: Int) =>
        ps.bindWith(user, limit) { rs =>
          rs.iterator() map { r => timelines.tweet(r) }
        }
    }

  }

  private def add_to_followers_timelines(tweet: UUID, user: UUID, created: DateTime)(implicit consistency: ConsistencyLevel): Future[Unit] = {
    followersOf(user) flatMap { followers =>
        followers.map { f =>
          prepared.timeline_append(f, tweet, created)(consistency)
              .execAsScala()
              .instrument()
        }.bundle.unit
    }
  }

  def post(t: Tweet)(implicit consistency: ConsistencyLevel): Future[UUID] = {
    for {
      _ <- tweets.insert()
          .consistencyLevel_=(consistency)
          .value(_.id, t.id)
          .value(_.user, t.user)
          .value(_.body, t.body)
          .value(_.created, t.created)
          .future()
          .instrument()
      _ <- add_to_followers_timelines(t.id, t.user, t.created)
    } yield t.id
  }

  def retweet(tweet: Tweet, retweeter: UUID)(implicit consistency: ConsistencyLevel): Future[Unit] = {
    // equivalent to:
    // if (retweets(tweet).add(retweeter)):
    //   for f in Followers(retweeter):
    //     timeline(f).add(tweet)
    {
      for {
        _ <- retweets(tweet.id).add(retweeter)
        _ <- add_to_followers_timelines(tweet.id, retweeter, tweet.created)
      } yield ()
    } recover {
      case _ => ()
    }
  }

  def getTweet(id: UUID)(implicit consistency: ConsistencyLevel): Future[Option[Tweet]] = {
    tweets.select.where(_.id eqs id).one() flatMap {
      case Some(tweet) =>
        {
          for {
            (userOpt, ct) <- users.select
                .consistencyLevel_=(consistency)
                .where(_.id eqs tweet.user)
                .one()
                .instrument() zip retweets(id).size().instrument(m.retweet_count)
          } yield for {
            u <- userOpt
          } yield {
            tweet.copy(retweets = ct.get, name = Some(u.name))
          }
        }.instrument(m.tweet_load)
      case None =>
        Future { None }
    }
  }


  def timeline(user: UUID, limit: Int): Future[IndexedSeq[Tweet]] =
    prepared.timeline_get(user, limit)(consistency).execAsScala()
        .flatMap { ts => ts.toIndexedSeq.map(getTweet).bundle }
        .map(_.flatten)


  val retwisOps = metrics.create.meter("retwis_op")

  object m {
    val user = metrics.create.timer("user")
    val follow = metrics.create.timer("follow")
    val tweet = metrics.create.timer("tweet")
    val retweet = metrics.create.timer("retweet")
    val timeline = metrics.create.timer("timeline")

    val retweet_count = metrics.create.timer("retweet_count")

    val timeline_length = metrics.create.histogram("timeline_length")

    val tweet_load = metrics.create.timer("tweet_load")
  }

  object Tasks {

    sealed abstract class Task(body: () => Future[Unit])
        extends (() => Future[Unit]) {
      def apply = body() map { _ => retwisOps.mark() }
    }

    case object NewUser extends Task(() =>
      store(randomUser()).unit.instrument(m.user).map { _ => nusers += 1 }
    )

    case object Follow extends Task(() =>
      follow(Uniform.user(), Zipf.user()).instrument(m.follow)
    )

    case object Unfollow extends Task(() => {
      val followee = Uniform.user()
      followers(followee).get(1).flatMap { fs =>
        fs.get map { follower =>
          unfollow(follower, followee)
        } bundle
      }.unit.instrument(m.follow)
    })

    case object Tweet extends Task(() => {
      post(randomTweet()) map { _ => () } instrument m.tweet
    })

    case object Timeline extends Task(() => {
      val user = Uniform.user()
      for {
        tweets <- timeline(user, limit = 10).instrument(m.timeline)
        _ <- tweets
            .filter(i => Random.nextDouble() > 0.9)
            .map { t =>
              retweet(t, user).instrument(m.retweet)
            }
            .bundle
      } yield {
        m.timeline_length << tweets.size
        ()
      }
    })

  }

  def generate(): Unit = {

    tables.map(_.create.ifNotExists().future()).bundle.await()
    Seq(retweets, followers, followees).map(_.create()).bundle.await()

    if (!config.do_reset) {
      println("# Skipped keyspace reset")
      if (config.do_generate) {
        println("# Truncating tables before generate")
        tables.map(_.truncate().future()).bundle.await()
        Seq(retweets, followers, followees).map(_.truncate()).bundle.await()
      }
    }

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
    val fTweets = (0 to nTweets) map { _ => post(randomTweet()) }
    fTweets.bundle.await()
    println("# Init complete.")
  }

  def workload(): Unit = {
    println(s"# Running workload for ${config.duration}")

    val mix = Map(
      Tasks.NewUser  -> 0.02,
      Tasks.Follow   -> 0.05,
      Tasks.Unfollow -> 0.03,
      Tasks.Tweet    -> 0.20,
      Tasks.Timeline -> 0.70
    )

    // only generate tasks as needed, limit parallelism
    implicit val ec = boundedQueueExecutionContext(
      workers = config.nthreads,
      capacity = config.cap
    )

    val actualDurationStart = Deadline.now
    val deadline = config.duration.fromNow
    val sem = new Semaphore(config.concurrent_reqs)

    while (deadline.hasTimeLeft &&
        sem.tryAcquire(deadline.timeLeft.inMillis, TimeUnit.MILLISECONDS)) {
      val f = weightedSample(mix)()

      f onSuccess { case _ => sem.release() }
      f onFailure { case e: Throwable =>
        Console.err.println(s"!! got an error: ${e.getMessage}")
        e.printStackTrace()
        sys.exit(1)
      }
    }

    val actualTime = actualDurationStart.elapsed
    output += ("actual_time" -> actualTime)
    println(s"# Done in ${actualTime.toSeconds}.${actualTime.toMillis%1000}s")
    println(s"# checking eventually correct")

    metrics.dump()
  }

}

class RetwisExec extends {
  override implicit val space = KeySpace("owl_retwis")
} with Retwis

object Workload extends RetwisExec {
  // override implicit lazy val session = Connector.throttledCluster.connect(space.name)
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