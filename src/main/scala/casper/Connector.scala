package casper

import java.net.InetAddress

import com.datastax.driver.core.Cluster
import com.typesafe.config.ConfigFactory
import com.websudos.phantom.connectors.KeySpace
import scala.collection.JavaConversions._

object Connector {
  val config = ConfigFactory.load()
  val hosts = config.getStringList("cassandra.host").map(InetAddress.getByName)
  val keyspace = KeySpace(config.getString("cassandra.keyspace"))
  val cluster = Cluster.builder().addContactPoints(hosts).build()
}
