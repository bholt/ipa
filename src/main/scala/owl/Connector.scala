package owl

import java.net.InetAddress

import com.datastax.driver.core.{HostDistance, PoolingOptions, Cluster, Session}
import com.typesafe.config.{ConfigRenderOptions, ConfigFactory}
import com.websudos.phantom.connectors.{KeySpace, SessionProvider}
import com.websudos.phantom.dsl.ConsistencyLevel

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.blocking
import scala.util.Try

object Connector {
  object config {
    val c = ConfigFactory.load()

    val hosts = c.getStringList("ipa.cassandra.host").map(InetAddress.getByName)
    def keyspace = c.getString("ipa.cassandra.keyspace")

    def do_reset = c.getBoolean("ipa.reset")
    def replication_factor = c.getInt("ipa.replication.factor")

    def consistency = {
      c.getString("ipa.consistency") match {
        case "strong" => ConsistencyLevel.ALL
        case "weak" => ConsistencyLevel.ONE
        case e => throw new RuntimeException(s"invalid consistency in config: $e")
      }
    }

    def nthreads = c.getInt("ipa.nthreads")
    def cap    = c.getInt("ipa.cap")
    def concurrent_reqs = c.getInt("ipa.concurrent.requests")
    def assumed_latency = Duration(c.getString("ipa.assumed.latency"))

    def do_generate   = c.getBoolean("ipa.retwis.generate")
    def zipf          = c.getDouble("ipa.retwis.zipf")
    def nUsers        = c.getInt("ipa.retwis.initial.users")
    def avgFollowers  = c.getInt("ipa.retwis.initial.followers")
    def tweetsPerUser = c.getInt("ipa.retwis.initial.tweets")
    def duration      = c.getInt("ipa.retwis.duration").seconds

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
  val config = Connector.config
  val cluster = Connector.cluster
  override implicit val space: KeySpace // = Connector.default_keyspace

  override implicit lazy val session = {
    println(">>> initializing session")
    cluster.newSession().init()
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

  def createKeyspace(s: Session): Unit = {
    val r = config.replication_factor
    println(s"# Creating keyspace: ${space.name} {replication_factor: $r}")
    blocking {
      s.execute(s"CREATE KEYSPACE IF NOT EXISTS ${space.name} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': $r};")
    }
  }

}
