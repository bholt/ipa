package ipa

import java.net.InetSocketAddress

import com.twitter.finagle.Service
import com.twitter.{util => tw}
import io.github.finagle.serial.scodec.ScodecSerial
import scodec.Codec
import scodec.codecs

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

import language.implicitConversions

case class User(name: String)

case class Greeting(user: User) {
  override def toString = s"Hello, ${user.name}!"
}

object GreetUser extends Service[User, Greeting] {
  override def apply(u: User): tw.Future[Greeting] =
    tw.Future.value(Greeting(u))
}

object Server {

  implicit val userCodec: Codec[User] =
    codecs.variableSizeBits(codecs.uint24, codecs.utf8).as[User]
  implicit val greetingCodec = userCodec.as[Greeting]

  implicit def twitterToScalaTry[T](t: tw.Try[T]): Try[T] = t match {
    case tw.Return(r) => Success(r)
    case tw.Throw(ex) => Failure(ex)
  }

  implicit def twitterToScalaFuture[T](f: tw.Future[T]): Future[T] = {
    val promise = Promise[T]()
    f.respond(promise complete _)
    promise.future
  }

  implicit class TwFuturePlus[T](f: tw.Future[T]) {
    def await(d: Duration = Duration.Inf): T = Await.result(f, d)
    def unit(implicit ec: ExecutionContext): Future[Unit] = f.map(_ => ())
  }

  def main(args: Array[String]) {
    val protocol = ScodecSerial[User, Greeting]
    val server = protocol.serve(new InetSocketAddress(12007), GreetUser)

    val client = protocol.newService("localhost:12007")

    client(User("Arthur"))
        .map { greeting => println(greeting) }
        .await()

    println("done")
  }
}
