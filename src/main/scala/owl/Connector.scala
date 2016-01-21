package owl

import java.net.InetAddress

import com.datastax.driver.core.{Cluster, Session}
import com.typesafe.config.{ConfigRenderOptions, ConfigFactory}
import com.websudos.phantom.connectors.{KeySpace, SessionProvider}
import com.websudos.phantom.dsl.ConsistencyLevel

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.blocking

object Connector {
  object config {
    val c = ConfigFactory.load()

    val hosts = c.getStringList("ipa.cassandra.host").map(InetAddress.getByName)
    def keyspace = c.getString("ipa.cassandra.keyspace")
    def replication_factor = c.getInt("ipa.cassandra.replication.factor")
    def consistency = {
      c.getString("ipa.owl.consistency") match {
        case "strong" => ConsistencyLevel.ALL
        case "weak" => ConsistencyLevel.ONE
        case e => throw new RuntimeException(s"invalid consistency in config: $e")
      }
    }

    def zipf = c.getDouble("ipa.retwis.zipf")

    def nUsers = c.getInt("ipa.retwis.initial.users")
    def avgFollowers = c.getInt("ipa.retwis.initial.followers")
    def tweetsPerUser = c.getInt("ipa.retwis.initial.tweets")

    def cap = c.getInt("ipa.owl.cap")
    def duration = c.getInt("ipa.retwis.duration").seconds

    def toJSON = {
      c.root().get("ipa").render(ConfigRenderOptions.concise().setFormatted(true))
    }
  }
  val cluster = Cluster.builder().addContactPoints(config.hosts).build()
  val keyspace = KeySpace(config.keyspace)
}

trait Connector extends SessionProvider {
  val config = Connector.config
  val cluster = Connector.cluster
  implicit val space = Connector.keyspace
  override implicit lazy val session = {
    val tmpSession = blocking { cluster.connect() }
    createKeyspace(tmpSession)
    blocking {
      val rs = tmpSession.execute(s"SELECT strategy_options FROM system.schema_keyspaces WHERE keyspace_name = '${space.name}'")
      val keyspace_options = rs.one().getString(0)
      println(s"# keyspace '${space.name}' options: $keyspace_options")
    }
    blocking {
      cluster.connect(space.name)
    }
  }

  def createKeyspace(s: Session): Unit = {
    val r = config.replication_factor
    blocking {
      s.execute(s"CREATE KEYSPACE IF NOT EXISTS ${space.name} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': $r};")
    }
  }

}
