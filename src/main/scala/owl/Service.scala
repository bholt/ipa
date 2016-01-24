package owl

import java.io.OutputStream
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.json.MetricsModule
import com.datastax.driver.core.utils.UUIDs
import com.datastax.driver.core.{ConsistencyLevel, Row}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.builder.primitives.Primitive
import com.websudos.phantom.column.DateTimeColumn
import com.websudos.phantom.dsl.{StringColumn, UUIDColumn, _}
import com.websudos.phantom.keys.PartitionKey
import nl.grons.metrics.scala.{FutureMetrics, InstrumentedBuilder}
import org.joda.time.DateTime

import scala.collection.JavaConversions._
import scala.concurrent.{Future, blocking}

// for Vector.sample
import owl.Util._

//////////////////
// User
case class User(
    id: UUID = UUID.randomUUID(),
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
  retweets: Long = 0,
  name: Option[String] = None
) {
  override def toString = {
    s"${name.getOrElse(user)}: $body [$created]"
  }
}

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

trait OwlService extends Connector with InstrumentedBuilder with FutureMetrics {
  lazy implicit val isession = Connector.cluster.connect(space.name)
  override val metricRegistry = new MetricRegistry

  object metric {

    val cassandraOpLatency = metrics.timer("cass_op_latency")
    val retwisOps = metrics.meter("retwis_op")

    private val mapper = new ObjectMapper()
        .registerModule(new MetricsModule(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, false))
        .registerModule(DefaultScalaModule)

    def write(out: OutputStream) = {
      val mConfig = config.c.root().withOnlyKey("ipa").unwrapped()
      val mMetrics = mapper.readValue(mapper.writeValueAsString(metricRegistry), classOf[java.util.Map[String,Object]])
      mapper.writerWithDefaultPrettyPrinter()
            .writeValue(out, mMetrics ++ mConfig)
    }

  }

  implicit class InstrumentedFuture[T](f: Future[T]) {
    def instrument(): Future[T] = {
      val ctx = metric.cassandraOpLatency.timerContext()
      f onComplete { _ => ctx.stop() }
      f
    }
  }

  trait TableGenerator {
    def createTables(): Future[Unit]
    def truncateTables(): Future[Unit]
  }

  class IPASet[K, V](name: String, consistency: ConsistencyLevel)
      (implicit evK: Primitive[K], evV: Primitive[V]) extends TableGenerator {

    case class Entry(key: K, value: V)
    class EntryTable extends CassandraTable[EntryTable, Entry] {
      object ekey extends PrimitiveColumn[EntryTable, Entry, K](this) with PartitionKey[K]
      object evalue extends PrimitiveColumn[EntryTable, Entry, V](this) with PrimaryKey[V]
      override val tableName = name
      override def fromRow(r: Row) = Entry(ekey(r), evalue(r))
    }

    case class Count(key: K, count: Long)
    class CountTable extends CassandraTable[CountTable, Count] {
      object ekey extends PrimitiveColumn[CountTable, Count, K](this) with PartitionKey[K]
      object ecount extends CounterColumn(this)
      override val tableName = name + "Count"
      override def fromRow(r: Row) = Count(ekey(r), ecount(r))
    }

    val entryTable = new EntryTable
    val countTable = new CountTable

    def createTables(): Future[Unit] = {
      Seq(entryTable, countTable).map(_.create.ifNotExists.future()).bundle.unit
    }

    def truncateTables(): Future[Unit] = {
      Seq(entryTable, countTable).map(_.truncate().future()).bundle.unit
    }

    /**
      * Local handle to a Set in storage; can be used like a Set
      *
      * @param key  identifier of this Set instance in storage
      */
    class Handle(key: K) {

      def contains(value: V): Future[Boolean] = {
        entryTable.select(_.evalue)
            .consistencyLevel_=(consistency)
            .where(_.ekey eqs key)
            .and(_.evalue eqs value)
            .one()
            .instrument()
            .map(_.isDefined)
      }

      def get(limit: Int = 0): Future[Iterator[V]] = {
        val q = entryTable
            .select
            .consistencyLevel_=(consistency)
            .where(_.ekey eqs key)

        val qlim = if (limit > 0) q.limit(limit) else q

        qlim.future().instrument().map { results =>
          results.iterator() map { row =>
            entryTable.fromRow(row).value
          }
        }
      }

      def add(value: V): Future[Boolean] = {
        this.contains(value) flatMap { dup =>
          if (dup) Future { false }
          else {
            for {
              _ <- countTable.update()
                  .consistencyLevel_=(consistency)
                  .where(_.ekey eqs key)
                  .modify(_.ecount += 1)
                  .future()
                  .instrument()
              _ <- entryTable.insert()
                  .consistencyLevel_=(consistency)
                  .value(_.ekey, key)
                  .value(_.evalue, value)
                  .future()
                  .instrument()
            } yield true
          }
        }
      }

      def remove(value: V): Future[Boolean] = {
        {
          for {
            removed <- contains(value)
            _ <- entryTable.delete()
                .consistencyLevel_=(consistency)
                .where(_.ekey eqs key)
                .and(_.evalue eqs value)
                .future()
                .instrument()
            if removed
            _ <- countTable.update()
                .consistencyLevel_=(consistency)
                .where(_.ekey eqs key)
                .modify(_.ecount -= 1)
                .future()
                .instrument()
          } yield true
        } recover {
          case _ => false
        }
      }

      def size(): Future[Long] = {
        countTable.select(_.ecount)
            .consistencyLevel_=(consistency)
            .where(_.ekey eqs key)
            .one()
            .map(opt => opt.getOrElse(0l))
            .instrument()
      }
    }

    def apply(key: K) = new Handle(key)
  }



  ///////////////////////
  // Other tables
  ///////////////////////
  val users = new Users
  val tweets = new Tweets
  val timelines = new Timelines

  val retweets = new IPASet[UUID, UUID]("retweets", config.consistency)
  val followers = new IPASet[UUID, UUID]("followers", config.consistency)
  val followees = new IPASet[UUID, UUID]("followees", config.consistency)

  object service {

    val tables = List(users, tweets, timelines)

    val FIRST_NAMES = Vector("Arthur", "Ford", "Tricia", "Zaphod")
    val LAST_NAMES = Vector("Dent", "Prefect", "McMillan", "Beeblebrox")

    def resetKeyspace(): Unit = {
      val tmpSession = blocking { cluster.connect() }
      if (config.do_reset) {
        println(s"# Resetting keyspace '${space.name}'")
        blocking {
          tmpSession.execute(s"DROP KEYSPACE IF EXISTS ${space.name}")
        }
      }

      createKeyspace(tmpSession)
      tables.map(_.create.ifNotExists().future()).bundle.await()
      Seq(retweets, followers, followees).map(_.createTables()).bundle.await()

      if (!config.do_reset) {
        println("# Skipped keyspace reset")
        if (config.do_generate) {
          println("# Truncating tables before generate")
          tables.map(_.truncate().future()).bundle.await()
          Seq(retweets, followers, followees).map(_.truncateTables()).bundle.await()
        }
      }
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

    def followersOf(user: UUID, limit: Int = 0): Future[Iterator[UUID]] = {
      followers(user).get(limit)
    }

    private def add_to_followers_timelines(tweet: UUID, user: UUID)(implicit consistency: ConsistencyLevel): Future[Unit] = {
      followersOf(user) flatMap { followers =>
        Future.sequence(followers map { f =>
          timelines.insert()
              .consistencyLevel_=(consistency)
              .value(_.user, f)
              .value(_.tweet, tweet)
              .future()
              .instrument()
        }).map(_ => ())
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
        _ <- add_to_followers_timelines(t.id, t.user)
      } yield t.id
    }

    def retweet(tweet: UUID, retweeter: UUID)(implicit consistency: ConsistencyLevel): Future[Option[UUID]] = {
      // equivalent to:
      // if (retweets(tweet).add(retweeter)):
      //   for f in Followers(retweeter):
      //     timeline(f).add(tweet)
      val f = for {
        added <- retweets(tweet).add(retweeter)
        if added
        _ <- add_to_followers_timelines(tweet, retweeter)
      } yield {
        Some(tweet)
      }
      f recover { case _ => None }
    }

    def getTweet(id: UUID)(implicit consistency: ConsistencyLevel): Future[Option[Tweet]] = {
      tweets.select.where(_.id eqs id).one() flatMap {
        case Some(tweet) =>
          for {
            userOpt <- users.select
                .consistencyLevel_=(consistency)
                .where(_.id eqs tweet.user)
                .one()
                .instrument()
            ct <- retweets(id).size()
          } yield for {
            u <- userOpt
          } yield {
            tweet.copy(retweets = ct, name = Some(u.name))
          }
        case None =>
          Future { None }
      }
    }

    def timeline(user: UUID, limit: Int)(implicit consistency: ConsistencyLevel) = {
      timelines.select
          .consistencyLevel_=(consistency)
          .where(_.user eqs user)
          .limit(limit)
          .future()
          .instrument()
          .flatMap { rs =>
            rs.iterator()
                .map(timelines.fromRow(_).tweet)
                .map(getTweet)
                .bundle
          }
          .map(_.flatten)
    }

  }
}
