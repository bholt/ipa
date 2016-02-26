package owl

import java.io.{ByteArrayOutputStream, PrintStream}
import java.util.UUID
import java.util.concurrent.{ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.datastax.driver.core.Session
import nl.grons.metrics.scala.Timer
import com.twitter.{util => tw}

import scala.collection.generic.CanBuildFrom
import scala.concurrent.duration.{Deadline, Duration}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.language.higherKinds
import scala.util.{Failure, Random, Success, Try}

object Util {

  def id(i: Int) = UUID.nameUUIDFromBytes(BigInt(i).toByteArray)

  implicit class IntToId(v: Int) {
    def id = Util.id(v)
  }

  implicit class VectorPlus[T](v: IndexedSeq[T]) {
    def sample = v(Random.nextInt(v.length))
  }

  implicit class FuturePlus[T](f: Future[T]) {
    def await(d: Duration = Duration.Inf): T = Await.result(f, d)
    def unit(implicit ec: ExecutionContext): Future[Unit] = f.map(_ => ())
  }

  implicit class TwAwaitablePlus[T](f: tw.Awaitable[T]) {
    def await(): T = tw.Await.result(f)
  }

  implicit class TwFuturePlus[T](f: tw.Future[T]) {
    def instrument(timer: Timer = null)(implicit metrics: IPAMetrics): tw.Future[T] = {
      val ctx = if (timer != null) timer.timerContext()
                else metrics.cassandraOpLatency.timerContext()
      f.onSuccess(_ => ctx.stop()).onFailure(_ => ctx.stop())
    }
  }

  implicit class ScalaToTwitterTry[T](t: Try[T]) {
    def asTwitter: tw.Try[T] = t match {
      case Success(r) => tw.Return(r)
      case Failure(ex) => tw.Throw(ex)
    }
  }

  implicit class TwitterToScalaTry[T](t: tw.Try[T]) {
    def asScala: Try[T] = t match {
      case tw.Return(r) => Success(r)
      case tw.Throw(ex) => Failure(ex)
    }
  }

  implicit class ScalaToTwitterFuture[T](f: Future[T])(implicit ec: ExecutionContext) {
    def asTwitter: tw.Future[T] = {
      val promise = tw.Promise[T]()
      f.onComplete(t => promise update t.asTwitter)
      promise
    }
  }

  implicit class TwitterToScalaFuture[T](f: tw.Future[T]) {
    def asScala: Future[T] = {
      val promise = Promise[T]()
      f.respond(t => promise complete t.asScala)
      promise.future
    }
  }

  implicit class FutureSeqPlus[A, M[X] <: TraversableOnce[X]](v: M[Future[A]]) {
    /**
      * Bundle up a bunch of futures into a single future using `Future.sequence`
      */
    def bundle(implicit cbf: CanBuildFrom[M[Future[A]], A, M[A]], executor: ExecutionContext): Future[M[A]] = Future.sequence(v)

    def firstCompleted(implicit ec: ExecutionContext): Future[A] = Future.firstCompletedOf(v)
  }

  implicit class TwFutureBundle[A, M[X] <: TraversableOnce[X]](fs: M[tw.Future[A]]) {
    def bundle(): tw.Future[Seq[A]] = tw.Future.collect(fs.toSeq)
  }

  implicit class InstrumentedFuture[T](f: Future[T])(implicit ec: ExecutionContext) {
    def instrument(timer: Timer = null)(implicit metrics: IPAMetrics) = {
      val ctx = if (timer != null) timer.timerContext()
                else metrics.cassandraOpLatency.timerContext()
      f.onComplete(_ => ctx.stop())
      f
    }
  }

  def combine(ma: Map[String, Any], mb: Map[String, Any]): Map[String,Any] = {
    (ma.keySet ++ mb.keySet) map { k =>
      k -> {
        (ma.get(k), mb.get(k)) match {
          case (Some(a: Map[String, Any]), Some(b: Map[String, Any])) =>
            combine(a, b)
          case (Some(a: Seq[_]), Some(b)) => a :+ b
          case (Some(a: Seq[_]), None) => a
          case (Some(a), None) => Seq(a)
          case (None, b: Seq[_]) => b
          case (None, Some(b)) => Seq(b)
          case (Some(a), Some(b)) => Seq(a, b)
          case _ => Seq()
        }
      }
    } toMap
  }

  implicit class DeadlinePlus(d: Deadline) {
    def elapsed = -d.timeLeft
  }

  implicit class StringPlus(s: String) {
    def toUUID = UUID.fromString(s)
  }

  implicit class SessionPlus(s: Session) {
    def nreplicas = s.getCluster.getMetadata.getAllHosts.size
  }

  class StringPrintStream(
      bos: ByteArrayOutputStream = new ByteArrayOutputStream
  ) extends PrintStream(bos) {
    def mkString = bos.toString("UTF8")
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
    println(s"$text".purple)
  }

  implicit class ColoredString(s: String) {
    def color(c: String) = c + s + Console.RESET

    def green  = color(Console.GREEN)
    def gray   = color(Console.BLACK)
    def yellow = color(Console.YELLOW)
    def red    = color(Console.RED)
    def purple = color(Console.MAGENTA)
    def cyan   = color(Console.CYAN)

    def bold = color(Console.BOLD)
  }

}
