package owl

import java.util.UUID

import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl.ConsistencyLevel
import org.apache.commons.math3.distribution.ZipfDistribution
import org.scalatest.Sequential
import scala.concurrent.duration._
import owl.Util._

import scala.util.Random

trait IPASetTestGeneric extends OwlTest {
  override implicit val space = KeySpace("owl_set_perf_test")
  implicit val consistency = ConsistencyLevel.ALL
  implicit override val patienceConfig =
    PatienceConfig(timeout = 500.millis, interval = 10.millis)

  implicit val ec = boundedQueueExecutionContext(
    workers = config.nthreads,
    capacity = config.cap
  )
  val n = 100
  val m = 1000

  def id(i: Int) = UUID.nameUUIDFromBytes(BigInt(i).toByteArray)

  val zipfDist = new ZipfDistribution(n, config.zipf)

  def zipfID() = id(zipfDist.sample())
  def urandID() = id(Random.nextInt(n))

  val timerAdd = metrics.timer("add_latency")
  val timerContains = metrics.timer("contains_latency")
  val timerSize = metrics.timer("size_latency")

  val mix = Map(
    'add -> 0.3,
    'contains -> 0.5,
    'size -> 0.2
  )

  def set: IPASet[UUID, UUID]

  def performanceTest() {
    println(">>> testing performance (zipf)")

    {
      for {
        i <- 0 to n
        j <- 0 to m
      } yield {
        val handle = set(zipfID())
        weightedSample(mix) match {
          case 'add =>
            handle.add(urandID()).instrument(timerAdd).unit
          case 'contains =>
            handle.contains(urandID()).instrument(timerContains).unit
          case 'size =>
            handle.size().instrument(timerSize).unit
        }
      }
    }.bundle.await()
  }

  def printMetrics(): Unit = {
    println(">>> printing metrics")
    metric.dump()
    metric.write(Console.err)
  }
}

class IPASetCollectionsPerf extends IPASetTestGeneric {

  override val set = new IPASetImplCollection[UUID, UUID]("sCol", config.consistency)

  "Collection-based set" should "be created" in {
    println(">>> creating collection-based set")
    set.create().await()
  }

  it should "test performance (zipf)" in {
    performanceTest()
  }

  it should "print metrics" in { printMetrics() }

}

class IPASetCounterPerf extends IPASetTestGeneric {

  override val set = new IPASetImplWithCounter[UUID, UUID]("sCounter", config.consistency)

  "Table-based set with counter" should "be created" in {
    println(">>> creating table-based set with counter")
    set.create().await()
  }

  it should "test performance (zipf)" in {
    performanceTest()
  }

  it should "print metrics" in { printMetrics() }
}

class IPASetPlainPerf extends IPASetTestGeneric {

  override val set = new IPASetImplPlain[UUID, UUID]("sPlain", config.consistency)

  "Table-based set" should "be created" in {
    println(">>> creating table-based set")
    set.create().await()
  }

  it should "test performance (zipf)" in {
    performanceTest()
  }

  it should "print metrics" in { printMetrics() }
}

class IPASetPerfSuite extends Sequential(
  new IPASetCollectionsPerf,
  new IPASetCounterPerf,
  new IPASetPlainPerf
)
