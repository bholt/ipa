package owl

import java.io.OutputStream
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{ConsoleReporter, MetricRegistry}
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
import nl.grons.metrics.scala.{Timer, FutureMetrics, InstrumentedBuilder}
import org.joda.time.DateTime

import scala.collection.JavaConversions._
import scala.concurrent.{Future, blocking}

// for Vector.sample
import owl.Util._

import scala.language.postfixOps

//////////////////
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

    def dump() = {
      ConsoleReporter.forRegistry(metricRegistry)
          .convertRatesTo(TimeUnit.SECONDS)
          .build()
          .report()
    }
  }

  implicit class InstrumentedFuture[T](f: Future[T]) {
    def instrument(m: Timer = metric.cassandraOpLatency): Future[T] = {
      val ctx = m.timerContext()
      f onComplete { _ => ctx.stop() }
      f
    }
  }

  trait TableGenerator {
    def create(): Future[Unit]
    def truncate(): Future[Unit]
  }

  abstract class IPASet[K, V] extends TableGenerator {

    def create(): Future[Unit]
    def truncate(): Future[Unit]
    def contains(key: K, value: V): Future[Boolean]
    def add(key: K, value: V): Future[Unit]
    def remove(key: K, value: V): Future[Unit]
    def size(key: K): Future[Int]

    /**
      * Local handle to a Set in storage; can be used like a Set
      *
      * @param key  identifier of this Set instance in storage
      */
    class Handle(key: K) {
      def contains(value: V) = IPASet.this.contains(key, value)
      def add(value: V) = IPASet.this.add(key, value)
      def remove(value: V) = IPASet.this.remove(key, value)
      def size() = IPASet.this.size(key)
    }

    def apply(key: K) = new Handle(key)
  }

  /**
    * IPASet implementation using a Cassandra Set collection column
    */
  class IPASetImplCollection[K, V](val name: String, val consistency: ConsistencyLevel)(implicit evK: Primitive[K], evV: Primitive[V]) extends IPASet[K, V] {

    case class Entry(key: K, value: Set[V])

    class EntryTable extends CassandraTable[EntryTable, Entry] {
      object ekey extends PrimitiveColumn[EntryTable, Entry, K](this) with PartitionKey[K]
      object evalue extends SetColumn[EntryTable, Entry, V](this) with Index[Set[V]]
      override val tableName = name
      override def fromRow(r: Row) = Entry(ekey(r), evalue(r))
    }

    val entryTable = new EntryTable

    override def create(): Future[Unit] = { entryTable.create.ifNotExists.future().unit }
    override def truncate(): Future[Unit] = { entryTable.truncate().future().unit }

    override def contains(key: K, value: V): Future[Boolean] = {
      entryTable.select.count()
          .consistencyLevel_=(consistency)
          .where(_.ekey eqs key)
          .and(_.evalue contains value)
          .one()
          .instrument()
          .map { ctOpt => ctOpt.exists(_ > 0) }
    }

    override def add(key: K, value: V): Future[Unit] = {
      entryTable.update()
          .consistencyLevel_=(consistency)
          .where(_.ekey eqs key)
          .modify(_.evalue.add(value))
          .future()
          .instrument()
          .unit
    }

    override def remove(key: K, value: V): Future[Unit] = {
      entryTable.update()
          .consistencyLevel_=(consistency)
          .where(_.ekey eqs key)
          .modify(_.evalue.remove(value))
          .future()
          .instrument()
          .unit
    }

    override def size(key: K): Future[Int] = {
      entryTable.select(_.evalue)
          .consistencyLevel_=(consistency)
          .where(_.ekey eqs key)
          .one()
          .instrument()
          .map { _.map(set => set.size).getOrElse(0) }
    }

    def get(key: K): Future[Set[V]] = {
      entryTable.select(_.evalue)
          .consistencyLevel_=(consistency)
          .where(_.ekey eqs key)
          .one()
          .map(_.getOrElse(Set[V]()))
    }

    def random(key: K): Future[V] = {
      get(key).map(_.toIndexedSeq.sample)
    }

    class Handle(key: K) extends super.Handle(key) {
      def get() = IPASetImplCollection.this.get(key)
      def random() = IPASetImplCollection.this.random(key)
    }
    override def apply(key: K) = new Handle(key)
  }

  class IPASetImplPlain[K, V](val name: String, val consistency: ConsistencyLevel)(implicit evK: Primitive[K], evV: Primitive[V]) extends IPASet[K, V] {

    case class Entry(key: K, value: V)
    class EntryTable extends CassandraTable[EntryTable, Entry] {
      object ekey extends PrimitiveColumn[EntryTable, Entry, K](this) with PartitionKey[K]
      object evalue extends PrimitiveColumn[EntryTable, Entry, V](this) with PrimaryKey[V]
      override val tableName = name
      override def fromRow(r: Row) = Entry(ekey(r), evalue(r))
    }

    val entryTable = new EntryTable

    override def create(): Future[Unit] =
      entryTable.create.ifNotExists.future().unit

    override def truncate(): Future[Unit] =
      entryTable.truncate.future().unit

    override def contains(key: K, value: V): Future[Boolean] = {
      entryTable.select(_.evalue)
          .consistencyLevel_=(consistency)
          .where(_.ekey eqs key)
          .and(_.evalue eqs value)
          .one()
          .instrument()
          .map(_.isDefined)
    }

    def get(key: K, limit: Int = 0): Future[Iterator[V]] = {
      val q = entryTable.select
          .consistencyLevel_=(consistency)
          .where(_.ekey eqs key)

      val qlim = if (limit > 0) q.limit(limit) else q

      qlim.future().instrument().map { results =>
        results.iterator() map { row =>
          entryTable.fromRow(row).value
        }
      }
    }

    override def add(key: K, value: V): Future[Unit] = {
      entryTable.insert()
                .consistencyLevel_=(consistency)
                .value(_.ekey, key)
                .value(_.evalue, value)
                .future()
                .instrument()
                .unit
    }

    override def remove(key: K, value: V): Future[Unit] = {
      entryTable.delete()
                .consistencyLevel_=(consistency)
                .where(_.ekey eqs key)
                .and(_.evalue eqs value)
                .future()
                .instrument()
                .unit
    }

    override def size(key: K): Future[Int] = {
      entryTable.select.count()
          .consistencyLevel_=(consistency)
          .where(_.ekey eqs key)
          .one()
          .map(_.getOrElse(0l).toInt)
          .instrument()
    }

    override def apply(key: K) = new Handle(key) {
      def get(limit: Int = 0): Future[Iterator[V]] =
        IPASetImplPlain.this.get(key, limit)
    }
  }

  class IPASetImplWithCounter[K, V](val name: String, val consistency: ConsistencyLevel)(implicit evK: Primitive[K], evV: Primitive[V]) extends IPASet[K, V] {

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

    def create(): Future[Unit] = {
      Seq(entryTable, countTable).map(_.create.ifNotExists.future()).bundle.unit
    }

    def truncate(): Future[Unit] = {
      Seq(entryTable, countTable).map(_.truncate().future()).bundle.unit
    }

    override def contains(key: K, value: V): Future[Boolean] = {
      entryTable.select(_.evalue)
          .consistencyLevel_=(consistency)
          .where(_.ekey eqs key)
          .and(_.evalue eqs value)
          .one()
          .instrument()
          .map(_.isDefined)
    }

    def get(key: K, limit: Int = 0): Future[Iterator[V]] = {
      val q = entryTable.select
            .consistencyLevel_=(consistency)
            .where(_.ekey eqs key)

      val qlim = if (limit > 0) q.limit(limit) else q

      qlim.future().instrument().map { results =>
        results.iterator() map { row =>
          entryTable.fromRow(row).value
        }
      }
    }

    override def add(key: K, value: V): Future[Unit] = {
      // dlog(s">>> $name($key).add($value)")
      this.contains(key, value) flatMap { dup =>
        if (dup) Future { () }
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
          } yield ()
        }
      }
    }

    override def remove(key: K, value: V): Future[Unit] = {
      {
        for {
          removed <- this.contains(key, value)
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
        } yield ()
      } recover {
        case _ => ()
      }
    }

    def size(key: K): Future[Int] = {
      countTable.select(_.ecount)
          .consistencyLevel_=(consistency)
          .where(_.ekey eqs key)
          .one()
          .map(o => o.getOrElse(0l).toInt)
          .instrument()
    }

    override def apply(key: K) = new Handle(key) {
      def get(limit: Int = 0): Future[Iterator[V]] =
        IPASetImplWithCounter.this.get(key, limit)
    }
  }



  ///////////////////////
  // Other tables
  ///////////////////////
  val users = new Users
  val tweets = new Tweets
  val timelines = new Timelines

  val retweets = new IPASetImplCollection[UUID, UUID]("retweets", config.consistency)
  val followers = new IPASetImplCollection[UUID, UUID]("followers", config.consistency)
  val followees = new IPASetImplCollection[UUID, UUID]("followees", config.consistency)

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
      Seq(retweets, followers, followees).map(_.create()).bundle.await()

      if (!config.do_reset) {
        println("# Skipped keyspace reset")
        if (config.do_generate) {
          println("# Truncating tables before generate")
          tables.map(_.truncate().future()).bundle.await()
          Seq(retweets, followers, followees).map(_.truncate()).bundle.await()
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

    def followersOf(user: UUID): Future[Iterator[UUID]] = {
      followers.get(user).map(_.toIterator)
    }

    private def add_to_followers_timelines(tweet: UUID, user: UUID, created: DateTime)(implicit consistency: ConsistencyLevel): Future[Unit] = {
      followersOf(user) flatMap { followers =>
        Future.sequence(followers map { f =>
          timelines.insert()
              .consistencyLevel_=(consistency)
              .value(_.user, f)
              .value(_.tweet, tweet)
              .value(_.created, created)
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
          .orderBy(_.tweet desc)
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
