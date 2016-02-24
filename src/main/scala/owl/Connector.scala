package owl

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import com.datastax.driver.core.{Cluster, HostDistance, PoolingOptions, Session}
import com.twitter.finagle.Thrift
import com.twitter.finagle.loadbalancer.Balancers
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, ConfigValue, ConfigValueType}
import com.websudos.phantom.connectors.{KeySpace, SessionProvider}
import com.websudos.phantom.dsl.ConsistencyLevel
import ipa.CommonImplicits
import ipa.thrift.ReservationService
import ipa.{thrift => th}
import com.twitter.{util => tw}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Future, blocking}
import scala.util.Try

object Connector {
  object config {
    val c = ConfigFactory.load()

    val disable_perf_tests = Try(c.getBoolean("ipa.disable.perf.tests")).toOption.getOrElse(false)

    val hosts = c.getStringList("ipa.cassandra.host").map(InetAddress.getByName)
    def keyspace = c.getString("ipa.cassandra.keyspace")

    def do_reset = c.getBoolean("ipa.reset")
    def replication_factor = c.getInt("ipa.replication.factor")

    private def consistencyFromString(s: String) = s match {
      case "strong" => ConsistencyLevel.ALL
      case "weak" => ConsistencyLevel.ONE
      case e => throw new RuntimeException(s"invalid consistency in config: $e")
    }

    def consistency = consistencyFromString(c.getString("ipa.consistency"))

    object reservations {
      val port = c.getInt("ipa.reservations.port")
    }

    object rawmix {
      val nsets = c.getInt("ipa.rawmix.nsets")
      val mix = c.getObject("ipa.rawmix.mix").toMap.map {
        case (key, value) => {
          (Symbol(key), value.unwrapped().toString.toDouble)
        }
      }
      // probability of doing consistency check
      // (only happens after adds, so scale up accordingly)
      val check_probability = c.getDouble("ipa.rawmix.check.probability") / mix('add)
    }

    object bound {
      private val split = c.getString("ipa.bound").split(":")

      val kind = split(0)

      val latency = kind match {
        case "latency" => Some(Duration(split(1)).asInstanceOf[FiniteDuration])
        case _ => None
      }

      val consistency = kind match {
        case "consistency" => Some(consistencyFromString(split(1)))
        case _ => None
      }

    }

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

  val cluster = Cluster.builder()
      .addContactPoints(config.hosts)
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

  implicit lazy val reservations: ReservationService[tw.Future] = {
    val cass_hosts = session.getCluster.getMetadata.getAllHosts.map(_.getAddress.getHostAddress)
    println(s"cassandra hosts: ${cass_hosts.mkString(", ")}")

    val port = config.reservations.port
    val hosts = cass_hosts.map(h => s"$h:$port").mkString(",")

    val service = Thrift.client
        .withSessionPool.maxSize(4)
        .withLoadBalancer(Balancers.aperture())
        .newServiceIface[th.ReservationService.ServiceIface](hosts, "ipa")

    Thrift.newMethodIface(service)
  }

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
    val r = config.replication_factor
    println(s"# Creating keyspace: ${space.name} {replication_factor: $r}")
    blocking {
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS ${space.name} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': $r};")
    }
  }

  def dropKeyspace()(implicit space: KeySpace, session: Session): Unit = {
    println(s"# Dropping keyspace: ${space.name}")
    blocking {
      session.execute(s"DROP KEYSPACE IF EXISTS ${space.name}")
    }
  }

}
