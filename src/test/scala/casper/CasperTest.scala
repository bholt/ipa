package casper

import com.datastax.driver.core.{ProtocolVersion, Session, ConsistencyLevel}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Inspectors, Matchers, FlatSpec}

abstract class CasperSpec extends FlatSpec with Matchers with Inspectors with ScalaFutures

class CasperTest extends CasperSpec with CasperService with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    service.createTables
  }

  override def afterAll(): Unit = {
    // service.cleanupTables
  }

  "Initialization" should "happen" in {
    println("My initialization ran!")
  }

  "A User" should "be inserted into Cassandra" in {
    val user = service.randomUser
    println(s"random user: $user")

//    val f = service.store(user)
    val protocolVersion = implicitly[Session].getCluster.getConfiguration.getProtocolOptions.getProtocolVersion

    println(s"protocol version: $protocolVersion")
    println(s"≥ v3? ${protocolVersion.compareTo(ProtocolVersion.V2)}")

    val f = users.model.insert
        .value(_.id, user.id)
        .value(_.username, user.username)
        .value(_.name, user.name)
        .value(_.created, user.created)
        .consistencyLevel_=(ConsistencyLevel.ALL)
        .future()

    whenReady(f) { result =>
      result.isExhausted shouldBe true
      result.wasApplied shouldBe true
      service.delete(user)
    }
    println(s"-- done with ${user.name}")

  }

}
