package owl

import java.util.concurrent.{ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}

import nl.grons.metrics.scala.Timer

import scala.collection.generic.CanBuildFrom
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.higherKinds
import scala.util.Random

object Util {

  implicit class VectorPlus[T](v: IndexedSeq[T]) {
    def sample = v(Random.nextInt(v.length))
  }

  implicit class FuturePlus[T](f: Future[T]) {
    def await(): T = Await.result(f, Duration.Inf)
    def unit(implicit ec: ExecutionContext): Future[Unit] = f.map(_ => ())
  }

  implicit class FutureSeqPlus[A, M[X] <: TraversableOnce[X]](v: M[Future[A]]) {
    /**
      * Bundle up a bunch of futures into a single future using `Future.sequence`
      */
    def bundle(implicit cbf: CanBuildFrom[M[Future[A]], A, M[A]], executor: ExecutionContext): Future[M[A]] = Future.sequence(v)
  }

  implicit class InstrumentedFuture[T](f: Future[T])(implicit ec: ExecutionContext) {
    def instrument(timer: Timer) = {
      val ctx = timer.timerContext()
      f.onComplete(_ => ctx.stop())
      f
    }
  }

  /** from scala.concurrent.impl.ExecutionContextImpl */
  def desiredParallelism = {
    def getInt(name: String, default: String) = (try System.getProperty(name, default) catch {
      case e: SecurityException => default
    }) match {
      case s if s.charAt(0) == 'x' => (Runtime.getRuntime.availableProcessors * s.substring(1).toDouble).ceil.toInt
      case other => other.toInt
    }

    def range(floor: Int, desired: Int, ceiling: Int) = scala.math.min(scala.math.max(floor, desired), ceiling)

    range(
      getInt("scala.concurrent.context.minThreads", "1"),
      getInt("scala.concurrent.context.numThreads", "x1"),
      getInt("scala.concurrent.context.maxThreads", "x1")
    )
  }

  /**
    * Execution context that throttles creation of futures by blocking threads generating futures once a queue reaches capacity.
    * http://quantifind.com/blog/2015/06/throttling-instantiations-of-scala-futures-1/
    */
  def boundedQueueExecutionContext(
    workers: Int = desiredParallelism,
    capacity: Int = desiredParallelism * 10
  ) = ExecutionContext.fromExecutorService(
    new ThreadPoolExecutor(
      workers, workers,
      0L, TimeUnit.SECONDS,
      new ArrayBlockingQueue[Runnable](capacity) {
        override def offer(e: Runnable) = {
          put(e); // may block if waiting for empty room
          true
        }
      }
    )
  )

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


  /**
    * Like 'println' but for debug output (colored)
    */
  def dlog(text: String) {
    println(s"${Console.BLACK}$text${Console.RESET}")
  }

  implicit class ColoredString(s: String) {
    def color(c: String) = c + s + Console.RESET

    def green  = color(Console.GREEN)
    def gray   = color(Console.BLACK)
    def yellow = color(Console.YELLOW)
    def red    = color(Console.RED)
    def purple = color(Console.MAGENTA)
    def cyan   = color(Console.CYAN)
  }

}
