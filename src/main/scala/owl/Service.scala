package owl

import java.util.UUID

import com.datastax.driver.core.Row
import com.datastax.driver.core.utils.UUIDs
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.column.DateTimeColumn
import com.websudos.phantom.dsl.{StringColumn, UUIDColumn}
import com.websudos.phantom.keys.PartitionKey
import org.joda.time.DateTime

import com.websudos.phantom.dsl._

import scala.collection.immutable.IndexedSeq
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

// for Vector.sample
import Util._

class Service {

  // Models


}

// User
case class User(id: UUID, username: String, name: String, created: DateTime)

class Users extends CassandraTable[Users, User] {

  object id extends UUIDColumn(this) with PartitionKey[UUID]
  object username extends StringColumn(this)
  object name extends StringColumn(this)
  object created extends DateTimeColumn(this)

  override def fromRow(r: Row) = User(id(r), username(r), name(r), created(r))

  object model extends Users {
    override val tableName = "users"
  }
}

trait OwlService extends Connector {

  val users = new Users

  object service {

    val FIRST_NAMES = Vector("Arthur", "Ford", "Tricia", "Zaphod")
    val LAST_NAMES = Vector("Dent", "Prefect", "McMillan", "Beeblebrox")

    def createTables(): Future[ResultSet] = {
      users.model.create.ifNotExists().future()
    }

    def cleanupTables() = {
      session.execute("DROP TABLE users;")
    }

    def randomUser: User = {
      val id = UUIDs.timeBased()
      User(id, s"u${id.hashCode()}", s"${FIRST_NAMES.sample} ${LAST_NAMES.sample}", DateTime.now())
    }

    def store(user: User): Future[ResultSet] = {
      users.model.insert
          .value(_.id, user.id)
          .value(_.username, user.username)
          .value(_.name, user.name)
          .value(_.created, user.created)
          .consistencyLevel_=(ConsistencyLevel.ALL)
          .future()
    }

    def getUserById(id: UUID): Future[Option[User]] = {
      users.model.select.where(_ => users.id eqs id).one()
    }

    def delete(user: User): Future[ResultSet] = {
      users.model.delete
          .where(_ => users.id eqs user.id)
          .statement
          .runWith(ConsistencyLevel.Any)
    }

    def initUsers(n: Int): Future[Boolean] = {
      val stores = (1 to n) map { i => store(randomUser) }
      Future.sequence(stores) map { rs =>
        rs.map(_.isExhausted).forall(identity)
      }
    }
  }
}
