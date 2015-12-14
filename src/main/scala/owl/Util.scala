package owl

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Random
import scala.concurrent.Await

object Util {

  implicit class VectorPlus[T](v: IndexedSeq[T]) {
    def sample = v(Random.nextInt(v.length))
  }

  def await[T](f : Future[T]): T = Await.result(f, Duration.Inf)

}
