package casper

import com.datastax.driver.core.{ResultSet, Session, ConsistencyLevel, Statement}
import com.websudos.phantom.dsl._

import scala.concurrent.Future
import scala.util.Random
import scala.collection.JavaConverters._

object Util {

  implicit class VectorPlus[T](v: Vector[T]) {
    def sample = v(Random.nextInt(v.length))
  }

  /**
    * Helper class to handle Cassandra consistency level operations from a statement
    */
  implicit class ConsistencyStatement(statement: Statement) {

    /**
      * Execute a statement from a session with a specific consistency level
      */
    def runWith(consistencyLevel: ConsistencyLevel)(implicit session: Session): Future[ResultSet] = {
      statement.setConsistencyLevel(consistencyLevel)
      Future(session.execute(statement))
    }

    /**
      * Execute a statement from a session with a specific consistency level to get a single [[Row]]
      */
    def getOneWith(consistencyLevel: ConsistencyLevel)(implicit session: Session): Row = {
      statement.setConsistencyLevel(consistencyLevel)
      session.execute(statement).one()
    }

    /**
      * Execute a statement from a session with a specific consistency level to get a List of [[Row]]
      */
    def getListWith(consistencyLevel: ConsistencyLevel)(implicit session: Session): List[Row] = {
      statement.setConsistencyLevel(consistencyLevel)
      session.execute(statement).all().asScala.toList
    }
  }
}
