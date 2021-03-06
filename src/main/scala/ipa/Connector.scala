package ipa

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import com.codahale.metrics.json.MetricsModule
import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, LatencyAwarePolicy, RoundRobinPolicy}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import com.websudos.phantom.connectors.{KeySpace, SessionProvider}
import com.websudos.phantom.dsl.ConsistencyLevel
import ipa.types.Bound
import org.joda.time.Period

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.blocking
import scala.concurrent.duration._
import scala.util.Try

object Connector {

  val json = new ObjectMapper()
      .registerModule(new MetricsModule(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, false))
      .registerModule(DefaultScalaModule)


  object config {
    val c = ConfigFactory.load()

    val disable_perf_tests = Try(c.getBoolean("ipa.disable.perf.tests")).toOption.getOrElse(false)

    val hosts = c.getStringList("ipa.cassandra.host").map(InetAddress.getByName)
    def keyspace = c.getString("ipa.cassandra.keyspace")

    def do_reset = c.getBoolean("ipa.reset")
    def replication = c.getString("ipa.replica.strategy")

    private def consistencyFromString(s: String) = s match {
      case "strong" => ConsistencyLevel.QUORUM
      case "weak" => ConsistencyLevel.ONE
      case e => throw new RuntimeException(s"invalid consistency in config: $e")
    }

    def consistency = consistencyFromString(c.getString("ipa.consistency"))

    object lease {
      val period = Duration(c.getString("ipa.lease.period")).asInstanceOf[FiniteDuration]
      val periodNanos = period.toNanos
      val periodMillis = period.toMillis
    }

    object reservations {
      val port = c.getInt("ipa.reservations.port")
      val lease = Duration(c.getString("ipa.reservations.lease"))
      val lease_period = Period.millis(lease.toMillis.toInt)
      val soon_period = Period.millis(lease.toMillis.toInt / 10)
    }

    private def getMixMap(field: String) =
      c.getObject(field).toMap.map {
        case (key, value) =>
          (Symbol(key), value.unwrapped().toString.toDouble)
      }

    object rawmix {
      val nsets = c.getInt("ipa.rawmix.nsets")

      val target = c.getInt("ipa.rawmix.target")

      val mix = getMixMap("ipa.rawmix.mix")
      // probability of doing consistency check
      // (only happens after adds, so scale up accordingly)
      val check_probability = c.getDouble("ipa.rawmix.check.probability") / mix('add)

      object counter {
        val mix = getMixMap("ipa.rawmix.counter.mix")
      }
    }

    object tickets {
      object initial {
        val venues = c.getInt("ipa.tickets.initial.venues")
        val events = c.getInt("ipa.tickets.initial.events")
        val remaining = c.getInt("ipa.tickets.initial.remaining")
      }
      val mix = getMixMap("ipa.tickets.mix")
    }

    lazy val bound = Bound.fromString(c.getString("ipa.bound"))

    def nthreads = c.getInt("ipa.nthreads")
    def cap    = c.getInt("ipa.cap")
    def concurrent_reqs = c.getInt("ipa.concurrent.requests")
    def assumed_latency = Duration(c.getString("ipa.assumed.latency"))

    def zipf          = c.getDouble("ipa.zipf")
    def duration      = c.getInt("ipa.duration").seconds

    def do_generate   = c.getBoolean("ipa.retwis.generate")
    def nUsers        = c.getInt("ipa.retwis.initial.users")
    def avgFollowers  = c.getInt("ipa.retwis.initial.followers")
    def tweetsPerUser = c.getInt("ipa.retwis.initial.tweets")

    def output_json = Try(c.getBoolean("ipa.output.json")).getOrElse(false)

    def toJSON = {
      c.root().get("ipa").render(ConfigRenderOptions.concise().setFormatted(true))
    }
  }

  val throttledCluster = Cluster.builder()
      .addContactPoints(config.hosts)
      .withPoolingOptions(new PoolingOptions()
          .setMaxRequestsPerConnection(HostDistance.LOCAL, config.concurrent_reqs)
          .setMaxRequestsPerConnection(HostDistance.REMOTE, config.concurrent_reqs))
      .build()

  val latencyMonitor =
    LatencyAwarePolicy.builder {
        sys.env.get("CASSANDRA_DC")
            .map { dc => DCAwareRoundRobinPolicy.builder().withLocalDc(dc).build() }
            .getOrElse {
              new RoundRobinPolicy
            }
      }
      .withRetryPeriod(5, TimeUnit.SECONDS)
      .build()

  val cluster = Cluster.builder()
      .addContactPoints(config.hosts)
      .withLoadBalancingPolicy(latencyMonitor)
      .build()

  val default_keyspace = KeySpace(config.keyspace)
}

trait Connector extends SessionProvider {
  def config = Connector.config
  val cluster = Connector.cluster
  override implicit val space: KeySpace // = Connector.default_keyspace

  val output = mutable.HashMap[String,AnyRef]()

  override implicit lazy val session = {
    println(">>> initializing session")
    cluster.newSession().init()
  }

  def nreplicas = session.getCluster.getMetadata.getAllHosts.size

  implicit lazy val reservations = new ReservationClient(cluster)

//  {
//    val tmpSession = blocking { cluster.connect() }
//    createKeyspace(tmpSession)
//    blocking {
//      val rs = tmpSession.execute(s"SELECT strategy_options FROM system.schema_keyspaces WHERE keyspace_name = '${space.name}'")
//      val keyspace_options = rs.one().getString(0)
//      println(s"# keyspace '${space.name}' options: $keyspace_options")
//    }
//    blocking {
//      cluster.connect(space.name)
//    }
//  }


  def createKeyspace(s: Session = null)(implicit space: KeySpace, defaultSession: Session): Unit = {
    val session = if (s != null) s else defaultSession
    val r = config.replication
    println(s"# create keyspace ${space.name} with $r")
    blocking {
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS ${space.name} WITH REPLICATION = $r;")
    }
  }

  def dropKeyspace()(implicit space: KeySpace, session: Session): Unit = {
    println(s"# Dropping keyspace: ${space.name}")
    blocking {
      session.execute(s"DROP KEYSPACE IF EXISTS ${space.name}")
    }
  }

}
