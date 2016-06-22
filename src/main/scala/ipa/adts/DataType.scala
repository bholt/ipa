package ipa.adts

import com.twitter.{util => tw}
import com.websudos.phantom.builder.query.ExecutableStatement
import com.websudos.phantom.dsl._
import ipa.Connector._
import ipa.thrift.{ReservationException, Table}
import ipa.types._
import ipa.{IPAMetrics, ReservationClient, TableGenerator}

import scala.concurrent._
import scala.util.{Failure, Success, Try}

case class CommonImplicits(implicit val session: Session, val space: KeySpace, val metrics: IPAMetrics, val reservations: ReservationClient)

case class Metadata(bound: Option[Bound] = None) {
  override def toString = bound map { b =>
    json.writeValueAsString(Map("bound" -> b.toString))
  } getOrElse {
    ""
  }
}

object Metadata {
  def fromString(s: String)(implicit imps: CommonImplicits) = {
    val m = json.readValue(s, classOf[Map[String,String]])
    Metadata(m.get("bound") map Bound.fromString)
  }
}

abstract class DataType(imps: CommonImplicits) extends TableGenerator {
  def name: String

  protected def table: Table = Table(space.name, name)

  /* metadata to store in the Cassandra table properties */
  def meta: Metadata = Metadata()

  implicit val session = imps.session
  implicit val space = imps.space
  implicit val metrics = imps.metrics
  implicit val reservations = imps.reservations
}

object DataType {
  def lookupMetadata(name: String)(implicit imps: CommonImplicits): Try[String] = {
    import imps._
    val query = s"SELECT comment FROM system_schema.tables WHERE keyspace_name = '${space.name}' AND table_name = '$name'"
    println(s"@> query: '$query'")
    Try {
      val row = blocking { session.execute(query).one() }
      val text = row.get("comment", classOf[String])
      text
    }
  }

  def createWithMetadata[T <: CassandraTable[T, _], E](name: String, tbl: T, metaStr: String)(implicit imps: CommonImplicits): tw.Future[Unit] = {
    import imps._
    DataType.lookupMetadata(name) filter { _ == metaStr } map {
      _ => tw.Future.Unit
    } recover { case e =>
      println(s">>> (re)creating ${space.name}.$name with metadata: '$metaStr'")
      session.execute(s"DROP TABLE IF EXISTS ${space.name}.$name")
      val stmt = tbl.create.`with`(comment eqs metaStr)
      println(s"@> ${stmt.queryString}")
      stmt.execute().unit
    } get
  }

  def fromName[T](name: String, fromNameAndBound: (String, Bound) => T)(implicit imps: CommonImplicits): Try[T] = {
    DataType.lookupMetadata(name) flatMap { metaStr =>
      val meta = Metadata.fromString(metaStr)
      meta.bound match {
        case Some(bound) =>
          Success(fromNameAndBound(name, bound))
        case _ =>
          Failure(ReservationException(s"Unable to find metadata for $name"))
      }
    } recoverWith {
      case e: Throwable =>
        Failure(ReservationException(s"metadata not found for $name"))
    }
  }
}


