package owl

import java.net.InetAddress

import com.datastax.driver.core.Cluster
import com.typesafe.config.ConfigFactory
import com.websudos.phantom.connectors.{SessionProvider, KeySpace}
import scala.collection.JavaConversions._

object Connector {
  val config = ConfigFactory.load()
  val hosts = config.getStringList("cassandra.host").map(InetAddress.getByName)
  val cluster = Cluster.builder().addContactPoints(hosts).build()
  val keyspace = KeySpace(config.getString("cassandra.keyspace"))
  val session = cluster.connect(keyspace.name)
}

trait Connector extends SessionProvider {
  val config = Connector.config
  val cluster = Connector.cluster
  implicit val space = Connector.keyspace
  override implicit lazy val session = Connector.session
}
