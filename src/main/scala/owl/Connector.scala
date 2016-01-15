package owl

import java.net.InetAddress

import com.datastax.driver.core.{Cluster, Session}
import com.typesafe.config.ConfigFactory
import com.websudos.phantom.connectors.{KeySpace, SessionProvider}
import com.websudos.phantom.dsl.ConsistencyLevel

import scala.collection.JavaConversions._
import scala.concurrent.blocking

object Connector {
  val config = ConfigFactory.load()
  val hosts = config.getStringList("cassandra.host").map(InetAddress.getByName)
  val cluster = Cluster.builder().addContactPoints(hosts).build()
  val keyspace = KeySpace(config.getString("cassandra.keyspace"))
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
    val r = config.getInt("cassandra.replication.factor")
    blocking {
      s.execute(s"CREATE KEYSPACE IF NOT EXISTS ${space.name} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': $r};")
    }
  }

  def configConsistency() =
    config.getString("owl.consistency") match {
      case "strong" => ConsistencyLevel.ALL
      case "weak" => ConsistencyLevel.ONE
      case c => throw new RuntimeException(s"invalid consistency in config: $c")
    }
}
