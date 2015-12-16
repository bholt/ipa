package owl

import com.codahale.metrics.MetricRegistry
import com.websudos.phantom.builder.query.ExecutableStatement
import nl.grons.metrics.scala.Timer

import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration.Duration
import scala.util.Random

object Util {

  implicit class VectorPlus[T](v: IndexedSeq[T]) {
    def sample = v(Random.nextInt(v.length))
  }

  implicit class FutureSeqPlus[T](v: Iterator[Future[T]]) {
    def bundle(implicit ec: ExecutionContext) = Future.sequence(v)
  }

  implicit class InstrumentedFuture[T](f: Future[T])(implicit ec: ExecutionContext) {
    def instrument(timer: Timer) = {
      val ctx = timer.timerContext()
      f.onComplete(_ => ctx.stop())
      f
    }
  }

  /**
    * Weighted random sample keys from map.
    * http://stackoverflow.com/a/24869852
    */
  final def weightedSample[A](dist: Map[A, Double]): A = {
    val p = scala.util.Random.nextDouble
    val it = dist.iterator
    var accum = 0.0
    while (it.hasNext) {
      val (item, itemProb) = it.next
      accum += itemProb
      if (accum >= p)
        return item  // return so that we don't have to search through the whole distribution
    }
    sys.error(f"this should never happen")  // needed so it will compile
  }

  /**
    * Helper to just block waiting for result of a Future.
    */
  def await[T](f : Future[T]): T = Await.result(f, Duration.Inf)

}
