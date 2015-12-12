package owl

import java.util.UUID

import com.datastax.driver.core.Row
import com.datastax.driver.core.utils.UUIDs
import com.websudos.phantom.{dsl, CassandraTable}
import com.websudos.phantom.column.DateTimeColumn
import com.websudos.phantom.dsl.{StringColumn, UUIDColumn}
import com.websudos.phantom.keys.PartitionKey
import org.joda.time.DateTime

import com.websudos.phantom.dsl._

import scala.collection.immutable.IndexedSeq
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConversions._

import org.apache.commons.math3.distribution.ZipfDistribution

// for Vector.sample
import Util._

//////////////////
// User
case class User(
    id: UUID = UUID.randomUUID(),
    username: String,
    name: String,
    created: DateTime = DateTime.now()
)

class Users extends CassandraTable[Users, User] {
  object id extends UUIDColumn(this) with PartitionKey[UUID]
  object username extends StringColumn(this)
  object name extends StringColumn(this)
  object created extends DateTimeColumn(this)

  override val tableName = "users"
  override def fromRow(r: Row) = User(id(r), username(r), name(r), created(r))
}

//////////////////
// Followers
case class Follower(user: UUID, follower: UUID)
case class Following(user: UUID, followee: UUID)

class Followers extends CassandraTable[Followers, Follower] {
  object user extends UUIDColumn(this) with PartitionKey[UUID]
  object follower extends UUIDColumn(this) with PrimaryKey[UUID]
  override val tableName = "followers"
  override def fromRow(r: Row) = Follower(user(r), follower(r))
}

class Followees extends CassandraTable[Followees, Following] {
  object user extends UUIDColumn(this) with PartitionKey[UUID]
  object following extends UUIDColumn(this) with PrimaryKey[UUID]
  override val tableName = "followees"
  override def fromRow(r: Row) = Following(user(r), following(r))
}


//////////////////
// Tweets
case class Tweet(
  id: UUID = UUID.randomUUID(),
  user: UUID,
  body: String,
  created: DateTime = DateTime.now(),
  retweets: Long = 0
)

class Tweets extends CassandraTable[Tweets, Tweet] {
  object id extends UUIDColumn(this) with PartitionKey[UUID]
  object user extends UUIDColumn(this)
  object body extends StringColumn(this)
  object created extends DateTimeColumn(this)
  override val tableName = "tweets"
  override def fromRow(r: Row) = Tweet(id(r), user(r), body(r), created(r))
}

case class TimelineEntry(user: UUID, tweet: UUID)
class Timelines extends CassandraTable[Timelines, TimelineEntry] {
  object user extends UUIDColumn(this) with PartitionKey[UUID]
  object tweet extends UUIDColumn(this) with PrimaryKey[UUID]
  override val tableName = "timelines"
  override def fromRow(r: Row) = TimelineEntry(user(r), tweet(r))
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

trait OwlService extends Connector {

  val users = new Users
  val followers = new Followers
  val followees = new Followees
  val tweets = new Tweets
  val timelines = new Timelines
  val retweets = new Retweets
  val retweetCounts = new RetweetCounts

  object service {

    val tables = List(users, followers, followees, tweets, timelines,
      retweets, retweetCounts)

    val FIRST_NAMES = Vector("Arthur", "Ford", "Tricia", "Zaphod")
    val LAST_NAMES = Vector("Dent", "Prefect", "McMillan", "Beeblebrox")

    def createTables(): Future[Unit] = {
      Future.sequence(
        tables.map(_.create.ifNotExists().future())
      ).map(_ => ())
    }

    def cleanupTables(): Unit = {
      tables.map(_.tableName).foreach(t => session.execute(s"DROP TABLE $t"))
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

    def store(user: User): Future[UUID] = {
      for {
        rs <- users.insert()
                   .value(_.id, user.id)
                   .value(_.username, user.username)
                   .value(_.name, user.name)
                   .value(_.created, user.created)
                   .consistencyLevel_=(ConsistencyLevel.ALL)
                   .future()
      } yield user.id
    }

    def getUserById(id: UUID): Future[Option[User]] = {
      users.select.where(_.id eqs id).one()
    }

    def delete(user: User): Future[ResultSet] = {
      users.delete
          .where(_.id eqs user.id)
          .statement
          .runWith(ConsistencyLevel.Any)
    }

    def follow(follower: UUID, followee: UUID): Future[Unit] = {
      for {
        r1 <- followers.insert()
            .value(_.user, followee)
            .value(_.follower, follower)
            .future()
        r2 <- followees.insert()
            .value(_.user, follower)
            .value(_.following, followee)
            .future()
      } yield ()
    }

    def unfollow(follower: UUID, followee: UUID): Future[Unit] = {
      for {
        r1 <- followers.delete()
            .where(_.user eqs followee)
            .and(_.follower eqs follower)
            .future()
        r2 <- followees.delete()
            .where(_.user eqs follower)
            .and(_.following eqs followee)
            .future()
      } yield ()
    }

    def userUUID(i: Int) = UUID.nameUUIDFromBytes(BigInt(i).toByteArray)

    def initSocialGraph(nUsers: Int, avgFollowers: Int, zipf: Double = 1.0): Future[Unit] = {
      val rnd = new ZipfDistribution(nUsers, zipf)

      // create users with UUIDs generated from numbers: 1..nUsers
      val users = (1 to nUsers) map { i => store(randomUser(id = userUUID(i))) }

      // follows (can run concurrently with users if needed
      val nFollows = nUsers * avgFollowers
      val follows = (1 to nFollows) map { _ =>
        val ids = rnd.sample(2).map(userUUID)
        follow(ids(0), ids(1))
      }

      Future.sequence(Seq(
        Future.sequence(users),
        Future.sequence(follows)
      )).map(_ => ())
    }

    def followersOf(user: UUID): Future[Iterator[UUID]] = {
      followers
          .select
          .where(_.user eqs user)
          .future() map { results =>
        for {
          row <- results.iterator()
        } yield followers.fromRow(row).follower
      }
    }

    private def add_to_followers_timelines(tweet: UUID, user: UUID) = {
      followersOf(user) flatMap { followers =>
        Future.sequence(followers map { f =>
          timelines.insert()
              .value(_.user, f)
              .value(_.tweet, tweet)
              .consistencyLevel_=(ConsistencyLevel.ALL)
              .future()
        })
      }
    }

    def post(t: Tweet): Future[UUID] = {
      for {
        _ <- tweets.insert()
                .value(_.id, t.id)
                .value(_.user, t.user)
                .value(_.body, t.body)
                .value(_.created, t.created)
                .consistencyLevel_=(ConsistencyLevel.ALL)
                .future()
        _ <- retweetCounts.update()
                .where(_.tweet eqs t.id)
                .modify(_.count += 0L) // force initialization (to 0)
                .future()
        _ <- add_to_followers_timelines(t.id, t.user)
      } yield t.id
    }

    private def hasRetweeted(tweet: UUID, user: UUID): Future[Boolean] = {
      retweets.select(_.retweeter)
          .where(_.tweet eqs tweet)
          .and(_.retweeter eqs user)
          .one()
          .map(_.isDefined)
    }

    def retweet(tweet: UUID, retweeter: UUID): Future[Option[UUID]] = {
      hasRetweeted(tweet, retweeter) flatMap { dup =>
        if (dup) Future { None }
        else {
          for {
            _ <- retweetCounts.update()
                .where(_.tweet eqs tweet)
                .modify(_.count += 1)
                .consistencyLevel_=(ConsistencyLevel.ALL)
                .future()
            _ <- retweets.insert()
                .value(_.tweet, tweet)
                .value(_.retweeter, retweeter)
                .consistencyLevel_=(ConsistencyLevel.ALL)
                .future()
            _ <- add_to_followers_timelines(tweet, retweeter)
          } yield Some(tweet)
        }
      }
    }

    def getRetweetCount(tweet: UUID): Future[Option[Long]] = {
      retweetCounts.select(_.count).where(_.tweet eqs tweet).one()
    }

    def getTweet(id: UUID): Future[Option[Tweet]] = {
      tweets.select.where(_.id eqs id).one() flatMap {
        case Some(tweet) =>
          retweetCounts
              .select(_.count)
              .where(_.tweet eqs id)
              .one()
              .map(ctOpt => ctOpt.map(ct => tweet.copy(retweets = ct)))
        case None =>
          Future { None }
      }
    }

    def timeline(user: UUID, limit: Int) = {
      timelines.select
          .where(_.user eqs user)
          .limit(limit)
          .future()
          .flatMap { rs =>
            Future.sequence(rs.iterator()
                .map(timelines.fromRow(_).tweet)
                .map(getTweet))
          }
          .map(_.flatten)
    }

  }
}
