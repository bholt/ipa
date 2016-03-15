package ipa.apps

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, Semaphore}

import com.datastax.driver.core.Row
import com.datastax.driver.core.utils.UUIDs
import com.websudos.phantom.builder.query.prepared.?
import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import com.websudos.phantom.keys.PartitionKey
import ipa.{BoundedCounter, IPACounter, IPAPool}
import org.apache.commons.math3.distribution.{NormalDistribution, UniformIntegerDistribution, ZipfDistribution}
import org.joda.time.DateTime
import owl.Consistency._
import owl.RawMixCounter._
import owl.{Interval, _}
import owl.Util._
import com.datastax.driver.core.{ConsistencyLevel => CLevel}
import com.twitter.util.{Future => TwFuture}
import owl.Connector.config.tickets.initial
import java.util.UUID

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.util.Random

object Gen {

  val CITIES = Vector("Albuquerque", "San Francisco", "Chicago", "New York", "Austin")

  val VENUE_TYPES = Vector("Hall", "Center", "Theater")

  val EVENT_TYPES = Vector("Con", "Symposium", "Concert", "Show")

  val WORDS = Vector("small batch", "Etsy", "axe", "plaid", "McSweeney's", "VHS", "viral", "cliche", "post-ironic", "health", "goth", "literally", "Austin", "brunch", "authentic", "hella", "street art", "Tumblr", "Blue Bottle", "readymade", "occupy", "irony", "slow-carb", "heirloom", "YOLO", "tofu", "ethical", "tattooed", "vinyl", "artisan", "kale", "selfie")

  def randomText(n: Int = 4): String = {
    (0 to n).map(_ => WORDS.sample).mkString(" ")
  }

  val randomCapacity: () => Int = {
    val mean = config.tickets.initial.remaining
    val dist = new NormalDistribution(mean, mean / 4)
    () => dist.sample().toInt
  }

  val randomVenue: () => Int = {
    val dist = new UniformIntegerDistribution(0, config.tickets.initial.venues)
    () => dist.sample()
  }

  // FIXME: this is a hack
  val venueMap = new ConcurrentHashMap[UUID, UUID]()
}

case class Venue(
    id: UUID = UUID.randomUUID(),
    name: String = s"${Gen.CITIES.sample} ${Gen.VENUE_TYPES.sample}",
    location: String = s"${Gen.CITIES.sample}",
    capacity: Int = Gen.randomCapacity()
)

class VenueTable extends CassandraTable[VenueTable, Venue] {
  object id extends UUIDColumn(this) with PartitionKey[UUID]
  object name extends StringColumn(this)
  object location extends StringColumn(this)
  object capacity extends IntColumn(this)
  override val tableName = "venues"
  override def fromRow(r: Row) = Venue(id(r), name(r), location(r), capacity(r))
}

case class Event(
    id: UUID,
    venue: UUID,
    name: String = s"${Gen.WORDS.sample.capitalize} ${Gen.EVENT_TYPES.sample} 2016",
    description: String = Gen.randomText(10),
    created: DateTime = DateTime.now()
) {
  override def toString = s"Event($id, venue: $venue, $name: $description)"
}

class EventTable extends CassandraTable[EventTable, Event] {
  object id extends UUIDColumn(this) with PrimaryKey[UUID] with ClusteringOrder[UUID] with Descending
  object venue extends UUIDColumn(this) with PartitionKey[UUID]
  object name extends StringColumn(this)
  object description extends StringColumn(this)
  object created extends DateTimeColumn(this) with ClusteringOrder[DateTime] with Ascending
  override val tableName = "events"
  override def fromRow(r: Row) = Event(id(r), venue(r), name(r), description(r), created(r))
}

class TicketSleuth(val duration: FiniteDuration) extends {
  override implicit val space = KeySpace("tickets")
} with OwlService {

  object m {
    val purchaseLatency = metrics.create.timer("op_purchase")
    val viewLatency = metrics.create.timer("op_view")
    val browseLatency = metrics.create.timer("op_browse")
    val createLatency = metrics.create.timer("op_create")

    val take_success = metrics.create.counter("take_success")
    val take_failed = metrics.create.counter("take_failure")

    val remaining = metrics.create.histogram("remaining")
    val remaining_error = metrics.create.histogram("remaining")
    val browse_size = metrics.create.histogram("browse_size")

    val op_errors = metrics.create.counter("op_errors")
  }

  val eventZipf: () => UUID = {
    val dist = new ZipfDistribution(initial.events, config.zipf)
    () => dist.sample().id
  }

  def urandID() = id(Random.nextInt(initial.events))

  val mix = config.tickets.mix

  val cdefault = config.consistency

  val venues = new VenueTable
  val events = new EventTable
  val tickets = IPAPool.fromNameAndBound("tickets", config.bound)

  object prepared {
    lazy val venueInsert: (Venue) => BoundOp[Unit] = {
      val ps = session.prepare(s"INSERT INTO ${space.name}.${venues.tableName} (${venues.id.name}, ${venues.name.name}, ${venues.location.name}, ${venues.capacity.name}) VALUES (?, ?, ?, ?)")
      (v: Venue) => ps.bindWith(v.id, v.name, v.location, v.capacity)(_ => ())(CLevel.QUORUM)
    }

    lazy val venueCapacity: (UUID) => (CLevel) => BoundOp[Int] = {
      val ps = session.prepare(s"SELECT ${venues.capacity.name} FROM ${space.name}.${venues.tableName} WHERE ${venues.id.name} = ?")
      (id: UUID) => ps.bindWith(id)(r => venues.capacity(r.first.get))
    }

    lazy val eventInsert: (Event) => (CLevel) => BoundOp[Unit] = {
      val ps = session.prepare(s"INSERT INTO ${space.name}.${events.tableName} (${events.id.name}, ${events.venue.name}, ${events.name.name}, ${events.description.name}, ${events.created.name}) VALUES (?, ?, ?, ?, ?)")
      (e: Event) => ps.bindWith(e.id, e.venue, e.name, e.description, e.created)(_ => ())
    }

    lazy val eventGet: (UUID, UUID) => (CLevel) => BoundOp[Event] = {
      val ps = session.prepare(s"SELECT * FROM ${space.name}.${events.tableName} WHERE ${events.id.name} = ? AND ${events.venue.name} = ? LIMIT 1")
      (id: UUID, venue: UUID) => ps.bindWith(id, venue)(rs => events.fromRow(rs.first.get))
    }

    lazy val eventsByVenue: (UUID, Int) => (CLevel) => BoundOp[Iterator[(UUID, String)]] = {
      val ps = session.prepare(s"SELECT ${events.id.name}, ${events.name.name} FROM ${space.name}.${events.tableName} WHERE ${events.venue.name} = ? LIMIT ?")
      (venue: UUID, limit: Int) => ps.bindWith(venue, limit) { rs =>
        rs.iterator().map { r => (events.id(r), events.name(r)) }
      }
    }
  }

  def generate(): Unit = {
    venues.create.ifNotExists().future().await()
    events.create.ifNotExists().future().await()
    tickets.create().await()

    Seq(venues.truncate().future().unit, events.truncate().future().unit).bundle.await()
    tickets.truncate().await()

    // generate venues
    println(s">>> generating ${initial.venues} venues")
    (0 to initial.venues)
        .map { i => prepared.venueInsert(Venue(i.id)).execAsTwitter() }
        .bundle().await()

    // generate events
    println(s">>> generating ${initial.events} events")
    (0 to initial.events)
        .map { i => createEvent(CLevel.QUORUM)(i.id)}
        .bundle().await()
  }

  def record(remaining: Inconsistent[Int]): Unit = {
    m.remaining << remaining.get
  }

  def createEvent(c: CLevel)(id: UUID): TwFuture[Unit] = {
    val e = Event(id, Gen.randomVenue().id)
    Gen.venueMap(id) = e.venue
    for {
      n <- prepared.venueCapacity(e.venue)(c).execAsTwitter()
      _ <- tickets.init(e.id, n)
      _ <- prepared.eventInsert(e)(c).execAsTwitter()
    } yield ()
  }

  def browseEventsByVenue(c: CLevel)(venue: UUID): TwFuture[Seq[String]] = {
    for {
      events <- prepared.eventsByVenue(venue, 10)(c).execAsTwitter()
//      results <- events map { case (e,name) =>
//                tickets(e).remaining().map(ct => (e, name, ct))
//              } bundle()
    } yield {
      m.browse_size << events.size
      events map { case (id, name) =>
        // record(ct)
        name
      } toSeq
    }
  }

  def viewEvent(c: CLevel)(id: UUID): TwFuture[String] = {
    for {
      (e, ct) <- prepared.eventGet(id, Gen.venueMap(id))(c).execAsTwitter() join
          tickets(id).remaining()
    } yield {
      record(ct)
      s"Event: ${e.name}, tickets remaining: ${ct.get}\n---\n${e.description}"
    }
  }

  def purchaseTicket(id: UUID): TwFuture[Seq[UUID]] = {
    for {
      success <- tickets(id).take(1)
    } yield {
      if (success.get.isEmpty) m.take_failed += 1
      else m.take_success += 1
      success.get
    }
  }

  def run() {

    val actualDurationStart = Deadline.now
    val deadline = duration.fromNow
    val sem = new Semaphore(config.concurrent_reqs)

    while (deadline.hasTimeLeft) {
      sem.acquire()
      val op = weightedSample(mix)
      val f = op match {
        case 'purchase =>
          purchaseTicket(eventZipf()).instrument(m.purchaseLatency)
        case 'view =>
          viewEvent(cdefault)(eventZipf()).instrument(m.viewLatency)
        case 'browse =>
          browseEventsByVenue(cdefault)(Gen.randomVenue().id).instrument(m.browseLatency)
        case 'create =>
          createEvent(cdefault)(UUID.randomUUID()).instrument(m.createLatency)
      }
      f onSuccess { case _ =>
        sem.release()
      } onFailure { case e: Throwable =>
        Console.err.println(s"Error with $op\n  ${e.getMessage}")
        m.op_errors += 1
      }
    }

    val actualTime = actualDurationStart.elapsed
    output += ("actual_time" -> actualTime)
    println(s">>> Done (${actualTime.toSeconds}.${actualTime.toMillis%1000}s)")
  }

}

object TicketSleuth extends {
  override implicit val space = KeySpace("tickets")
} with Connector {

  def main(args: Array[String]): Unit = {
    if (config.do_reset) dropKeyspace()
    createKeyspace()

    val warmup = new TicketSleuth(5 seconds)
    warmup.generate()

    println(s">>> Warmup (${warmup.duration})")
    warmup.run()

    reservations.clients.values.map(_.metricsReset()).bundle().await()

    val workload = new TicketSleuth(config.duration)
    println(s">>> Workload (${workload.duration})")
    workload.run()
    workload.metrics.dump()

    sys.exit()
  }

}
