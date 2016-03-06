package owl

import java.io.{ByteArrayOutputStream, PrintStream}
import java.net.InetAddress
import java.util.UUID
import java.util.concurrent.{ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.codahale.metrics
import com.codahale.metrics.Timer
import com.datastax.driver.core._
import com.google.common.util.concurrent.{FutureCallback, Futures}
import com.twitter.util.{Return, Throw}
import com.twitter.{util => tw}
import com.websudos.phantom.Manager
import com.websudos.phantom.builder.query.prepared.{ExecutablePreparedQuery, PreparedBlock}
import com.websudos.phantom.connectors.KeySpace
import ipa.thrift.Table
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import scala.collection.generic.CanBuildFrom
import scala.concurrent.duration.{Deadline, Duration}
import scala.concurrent._
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

  implicit class TablePlus(t: Table) {
    def fullname = s"${t.space}.${t.name}"
  }

  implicit class TwAwaitablePlus[T](f: tw.Awaitable[T]) {
    def await(): T = tw.Await.result(f)
  }

  implicit class TwFuturePlus[T](f: tw.Future[T]) {
    def instrument(timer: Timer = null)(implicit metrics: IPAMetrics): tw.Future[T] = {
      val ctx = if (timer != null) timer.time()
                else metrics.cassandraOpLatency.time()
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
      val p = tw.Promise[T]()
      f onComplete {
        case Success(v) => p.setValue(v)
        case Failure(e) => p.setException(e)
      }
      p
    }
  }

  implicit class TwitterToScalaFuture[T](f: tw.Future[T]) {
    def asScala: Future[T] = {
      val p = Promise[T]()
      f respond {
        case tw.Return(v) => p success v
        case tw.Throw(e) => p failure e
      }
      p.future
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
      val ctx = if (timer != null) timer.time()
                else metrics.cassandraOpLatency.time()
      f onComplete { _ => ctx.stop() }
      f onFailure { case e: Exception => Console.err.println(e); throw e }
      f
    }
  }

  implicit class ExecutablePreparedQueryPlus(ps: ExecutablePreparedQuery) {

    def futureTwitter(c: ConsistencyLevel = ps.options.consistencyLevel.orNull)(implicit session: Session): tw.Future[ResultSet] = {
      val stmt = new SimpleStatement(ps.qb.terminate().queryString)
          .setConsistencyLevel(c)
      stmt.execAsTwitter()
    }

    def futureScala(c: ConsistencyLevel = ps.options.consistencyLevel.orNull)(implicit session: Session): Future[ResultSet] = {
      val stmt = new SimpleStatement(ps.qb.terminate().queryString)
          .setConsistencyLevel(c)
      stmt.execAsScala()
    }
  }

  implicit class FutureResultPlus(f: Future[ResultSet]) {
    def first[T](convert: Row => T)(implicit ec: ExecutionContext) = {
      f map { rs => Option(rs.one()).map(convert) }
    }
  }

  val this_host: String = InetAddress.getLocalHost.getHostAddress

  // note: this is actually lossless (they just pack the 4 bytes in)
  val this_host_hash: Int = InetAddress.getLocalHost.hashCode

  implicit class TwFutureResultPlus(f: tw.Future[ResultSet]) {
    def first[T](convert: Row => T)(implicit ec: ExecutionContext) = {
      f map { rs => Option(rs.one()).map(convert) }
    }
  }

  implicit class PreparedStatementPlus(ps: PreparedStatement) {
    def bindWith(args: Any*)(c: ConsistencyLevel) = {
      def flatten(p: Any): AnyRef = p match {
        case Some(x) => flatten(x)
        case None => null.asInstanceOf[AnyRef]
        case x: List[_] => x.asInstanceOf[List[Any]].asJava
        case x: Set[_] => x.asInstanceOf[Set[Any]].asJava
        case x: Map[_, _] => x.asInstanceOf[Map[Any, Any]].asJava
        case x: DateTime => x.toDate
        case x: Enumeration#Value => x.asInstanceOf[Enumeration#Value].toString
        case x: BigDecimal => x.bigDecimal
        case x: BigInt => x.bigInteger
        case x => x.asInstanceOf[AnyRef]
      }
      ps.setConsistencyLevel(c).bind(args map flatten :_*)
    }
  }

  implicit class StatementPlus(stmt: Statement) {
    def execAsTwitter()(implicit session: Session): tw.Future[ResultSet] = {
      val promise = tw.Promise[ResultSet]()
      val future = session.executeAsync(stmt)
      val callback = new FutureCallback[ResultSet] {
        def onSuccess(result: ResultSet): Unit = {
          promise update Return(result)
        }
        def onFailure(err: Throwable): Unit = {
          Manager.logger.error(err.getMessage)
          promise update Throw(err)
        }
      }
      Futures.addCallback(future, callback, Manager.executor)
      promise
    }

    def execAsScala()(implicit session: Session): Future[ResultSet] = {
      val promise = Promise[ResultSet]()
      val future = session.executeAsync(stmt)
      val callback = new FutureCallback[ResultSet] {
        def onSuccess(result: ResultSet): Unit = {
          promise success result
        }

        def onFailure(err: Throwable): Unit = {
          Manager.logger.error(err.getMessage)
          promise failure err
        }
      }
      Futures.addCallback(future, callback, Manager.executor)
      promise.future
    }
  }

  def combine(ma: Map[String, Any], mb: Map[String, Any]): Map[String,Any] = {
    (ma.keySet ++ mb.keySet) map { k =>
      k -> {
        (ma.get(k), mb.get(k)) match {
          case (Some(a: Map[_, _]), Some(b: Map[_, _])) =>
            combine(a.asInstanceOf[Map[String,Any]], b.asInstanceOf[Map[String,Any]])
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

  implicit def cellToMetric[T](cell: MetricCell[T]): T = cell.metric

  implicit class CounterMetricPlus(c: MetricCell[metrics.Counter]) {
    def +=(v: Long): Unit = c.inc(v)
    def -=(v: Long): Unit = c.dec(v)
  }

  implicit class HistogramMetricPlus(h: MetricCell[metrics.Histogram]) {
    def +=(v: Long): Unit = h.update(v)
    def <<(v: Long): Unit = h.update(v)
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
