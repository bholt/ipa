package owl

import java.util.UUID

import com.datastax.driver.core.Row
import com.datastax.driver.core.utils.UUIDs
import com.websudos.phantom.CassandraTable
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
  created: DateTime = DateTime.now()
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

trait OwlService extends Connector {

  val users = new Users
  val followers = new Followers
  val followees = new Followees
  val tweets = new Tweets
  val timelines = new Timelines

  object service {

    val tables = List(users, followers, followees, tweets, timelines)

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

    def randomUser: User = {
      val id = UUIDs.timeBased()
      User(id, s"u${id.hashCode()}", s"${FIRST_NAMES.sample} ${LAST_NAMES.sample}", DateTime.now())
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

    def initUsers(nUsers: Int, avgFollowers: Int): Future[Unit] = {
      val nFollows = nUsers * avgFollowers
      val ids = (1 to nUsers) map { _ => store(randomUser) }
      Future.sequence(ids) flatMap { ids =>
        val fs = (1 to nFollows) map { _ => follow(ids.sample, ids.sample) }
        Future.reduce(fs){ case (_,_) => () }
      }
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

    def post(t: Tweet): Future[UUID] = {
      val followersFuture = for {
        _ <- tweets.insert()
                   .value(_.id, t.id)
                   .value(_.user, t.user)
                   .value(_.body, t.body)
                   .value(_.created, t.created)
                   .consistencyLevel_=(ConsistencyLevel.ALL)
                   .future()
        followers <- followersOf(t.user)
      } yield followers

      followersFuture flatMap { fs =>
        Future.sequence(fs map { f =>
          timelines.insert()
                   .value(_.user, f)
                   .value(_.tweet, t.id)
                   .future()
        })
      } map { _ =>
        t.id
      }
    }

    def getTweet(id: UUID): Future[Option[Tweet]] = {
      tweets.select.where(_.id eqs id).one()
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
