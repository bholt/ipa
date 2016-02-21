package ipa

import java.util.UUID

import com.twitter.finagle.Thrift
import com.twitter.{util => tw}
import ipa.Counter.ErrorTolerance
import owl.Tolerance
import owl.Util._

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class ReservationService extends thrift.ReservationService[Future] {

  val tables = new mutable.HashMap[String, Counter with ErrorTolerance]

  /**
    * Initialize new UuidSet
    * TODO: make generic version
    */
  override def createUuidset(name: String, sizeTolerance: Double): Future[Unit] = ???

  /** Initialize new Counter table. */
  override def createCounter(table: String, error: Double): Future[Unit] = {
    val counter = new Counter with ErrorTolerance {
      def name = table
      def tolerance = Tolerance(error)
    }
    tables += (table -> counter)
    counter.create()
  }

  override def read(name: String, key: String): Future[Long] = {
    val counter = tables(name)
    // TODO: Figure out how to return Interval[T]
    // - See if we can return a generic Inconsistent[T] and downcast on the client
    // - Otherwise, create Interval thrift type to shadow the real one
    counter(UUID.fromString(key)).read() map { _.get }
  }

  override def incr(name: String, key: String, by: Long): Future[Unit] = ???
}

object ReservationService {
  def main(args: Array[String]): Unit = {
    val host = "localhost:14007"
    val server = Thrift.serveIface(
      host,
      new thrift.LoggerService[tw.Future] {

        def log(message: String, logLevel: Int): tw.Future[String] = {
          println(s"[$logLevel] Server received: '$message'")
          tw.Future.value(s"You've sent: ('$message', $logLevel)")
        }

        var counter = 0

        // getLogSize throws ReadExceptions every other request.
        def getLogSize(): tw.Future[Int] = {
          counter += 1
          if (counter % 2 == 1) {
            println(s"Server: getLogSize ReadException")
            tw.Future.exception(new ReadException())
          } else {
            println(s"Server: getLogSize Success")
            tw.Future.value(4)
          }
        }

      }
    )

    val clientService = Thrift.newServiceIface[LoggerService.ServiceIface](host, "ipa")

    val client = Thrift.newMethodIface(clientService)
    client.log("hello", 1) map { println(_) } await()
  }
}
