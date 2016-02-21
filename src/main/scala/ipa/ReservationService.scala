package ipa

import com.twitter.finagle.Thrift
import com.twitter.{util => tw}
import ipa.LoggerService.Log

import owl.Util._

class ReservationService extends thrift.ReservationService {

}

object ReservationService {
  def main(args: Array[String]): Unit = {
    val host = "localhost:14007"
    val server = Thrift.serveIface(
      host,
      new LoggerService[tw.Future] {

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
    client.log("hello", 1)
  }
}
