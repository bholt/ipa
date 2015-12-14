package owl

import java.net.InetAddress

import com.datastax.driver.core.{Session, Cluster}
import com.typesafe.config.ConfigFactory
import com.websudos.phantom.connectors.{SessionProvider, KeySpace}
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
    val replication = config.getString("cassandra.replication.command")
    blocking {
      tmpSession.execute(s"CREATE KEYSPACE IF NOT EXISTS ${space.name} WITH replication = $replication;")
    }
    blocking {
      cluster.connect(space.name)
    }
  }
}
