# Disciplined Inconsistency
## Type system
- specify *lower bound* of error (e.g. the *most precise you will ever ask it to be*) for read operations
- this frees the underlying implementation up to be weaker
- for example:
	- 

## Evaluation
- 


~~~scala

class Stale[T] {
  def consistency: ConsistencyLevel // e.g. 'ALL' or 'ONE' (or strong/weak)
  
  // most recent time when we heard from all replicas (consensus point?)
  def recency: DateTime
}

class IPASet[V] {
  def      add(value: V): Unit
  def   remove(value: V): Unit
  def contains(value: V): Boolean
  def     size():         Int

  def      add(level: Consistency)(value: V): Unit
  def contains(level: Consistency)(value: V): Inconsistent[T]
  def     size(level: Consistency)():         Inconsistent[T]
  
  def contains(bound: Duration)(value: V): Stale[Boolean]
  def     size(bound: Duration)():         Stale[Boolean]

  def contains(bound: Probability)(value: V): Probabilistic[Boolean]
  
  def size(bound: Tolerance)(value: V): Interval[Int]
}

val retweets = new IPASet {
  def add(strong)
  def add(<1%)
  
  def add(_)
  
  // lower bound
  // anything that can meet this is a valid impl
  // frees up your impl to be weaker
  def contains(>1% prob) // contains(>1%), never need to be more precise than 1%
  
  // upper bound on error
  // - value doesn't make sense if it doesn't meet this
  // - enforces contract on clients?
  def contains() // contains(<1%) -> writes need to be able to support precise reads
  
  def size(1% error)
}

class IPAList[V] {
  def append(value: V): Unit
  def slice(start: Int, end: Int): List[V]

  def slice(bound: Duration)(start: Int, end: Int): Stale[List[V]]

}

object Workload {
  def loadTimeline() {
    val t: Inconsistent[List[Tweet]] = timeline.get(weak)("Brandon")
        
    // make it consistent
    // could just call the method with strong consistency
    // -- *don't* get new data with this version
    // - what it would have been if i'd called it consistently
    wait(t) => List[Tweet]
    
    // instant cast
    endorse(t) => List[Tweet]
    println("This may be wrong!")
    
    // preview your shopping cart
    
    s.add(strong)("a")
    s.contains(weak)("a") // ok
    
    s.add(weak)("a")
    s.contains(strong)("a") // ok
    
    val r = s.contains(100 millis)("a")
    if (r.weak()) {
      println("may be wrong")
    }
    r.consistency match {
      case strong => ...
      case weak => ...
    }
    
  }
}
~~~

## Parameterizable ADT
~~~scala

class IPASet[K, V]() {
	class EntryTable
}

~~~










Comparing performance of different Set implementations.

~~~js
{
  "owl.IPASetCollectionsPerf.add_latency" : {
    "rate_units" : "calls/second",
    "count" : 30445,
    "duration_units" : "milliseconds",
    "p75" : 2605.73535375,
    "m1_rate" : 238.54163467389006,
    "mean" : 2217.5476543959144,
    "min" : 70.54809,
    "p95" : 4421.783537349999,
    "m15_rate" : 31.667250835114565,
    "max" : 6050.761719,
    "stddev" : 970.9673491372347,
    "m5_rate" : 85.01721820735608,
    "p99" : 5558.44677964,
    "p98" : 5078.3113306,
    "mean_rate" : 215.71432720225994,
    "p50" : 2166.4828079999997,
    "p999" : 6050.386874744
  },
  "owl.IPASetCollectionsPerf.cass_op_latency" : {
    "rate_units" : "calls/second",
    "count" : 101101,
    "duration_units" : "milliseconds",
    "p75" : 2667.59034175,
    "m1_rate" : 1058.967464665505,
    "mean" : 2350.775244909533,
    "min" : 101.05494499999999,
    "p95" : 4574.041385749999,
    "m15_rate" : 1426.9769789909533,
    "max" : 6189.657684,
    "stddev" : 984.3702636624531,
    "m5_rate" : 1343.2984491475052,
    "p99" : 5561.92497876,
    "p98" : 5202.683537239997,
    "mean_rate" : 1013.9881463369734,
    "p50" : 2210.4991365,
    "p999" : 6189.565839608
  },
  "owl.IPASetCollectionsPerf.contains_latency" : {
    "rate_units" : "calls/second",
    "count" : 50452,
    "duration_units" : "milliseconds",
    "p75" : 2673.3752385,
    "m1_rate" : 393.5931176118732,
    "mean" : 2378.642269905642,
    "min" : 102.776502,
    "p95" : 4667.139654399998,
    "m15_rate" : 52.36314086334172,
    "max" : 6236.609394,
    "stddev" : 972.7098225915943,
    "m5_rate" : 140.5488661485897,
    "p99" : 5611.6290565,
    "p98" : 5118.823729839999,
    "mean_rate" : 357.4684494792317,
    "p50" : 2223.6371934999997,
    "p999" : 6235.999945109
  },
  "owl.IPASetCollectionsPerf.size_latency" : {
    "rate_units" : "calls/second",
    "count" : 20204,
    "duration_units" : "milliseconds",
    "p75" : 2713.81183875,
    "m1_rate" : 158.4286305829024,
    "mean" : 2444.165944864786,
    "min" : 97.573949,
    "p95" : 4977.410453299999,
    "m15_rate" : 20.957099709962602,
    "max" : 6236.359474,
    "stddev" : 1073.7791351918152,
    "m5_rate" : 56.29735962157958,
    "p99" : 5852.165442150005,
    "p98" : 5570.13959516,
    "mean_rate" : 143.15105400929005,
    "p50" : 2265.297595,
    "p999" : 6235.102009611
  }
}
{
  "owl.IPASetCounterPerf.add_latency" : {
    "rate_units" : "calls/second",
    "count" : 30152,
    "duration_units" : "milliseconds",
    "p75" : 948.8483865,
    "m1_rate" : 356.15849669239356,
    "mean" : 747.3417193005836,
    "min" : 65.42851999999999,
    "p95" : 1846.047398249999,
    "m15_rate" : 28.66747422031725,
    "max" : 2638.3673949999998,
    "stddev" : 510.62535510664736,
    "m5_rate" : 83.64330517655317,
    "p99" : 2230.142375570001,
    "p98" : 2078.2767350399995,
    "mean_rate" : 141.34137778373756,
    "p50" : 615.312223,
    "p999" : 2637.5918036880003
  },
  "owl.IPASetCounterPerf.cass_op_latency" : {
    "rate_units" : "calls/second",
    "count" : 116937,
    "duration_units" : "milliseconds",
    "p75" : 666.019462,
    "m1_rate" : 3510.0754069220316,
    "mean" : 510.4500209727626,
    "min" : 37.419889,
    "p95" : 959.4648897499999,
    "m15_rate" : 3325.093874852212,
    "max" : 1556.729464,
    "stddev" : 260.89522856768497,
    "m5_rate" : 3355.386544535928,
    "p99" : 1229.49319682,
    "p98" : 1142.6898306599992,
    "mean_rate" : 3778.7193185250726,
    "p50" : 506.06982949999997,
    "p999" : 1555.228169554
  },
  "owl.IPASetCounterPerf.contains_latency" : {
    "rate_units" : "calls/second",
    "count" : 50728,
    "duration_units" : "milliseconds",
    "p75" : 672.043188,
    "m1_rate" : 600.8198625585626,
    "mean" : 514.5788691391051,
    "min" : 41.520925999999996,
    "p95" : 907.9735317999998,
    "m15_rate" : 48.55676080386127,
    "max" : 1510.051061,
    "stddev" : 242.05372589176704,
    "m5_rate" : 141.58694667406647,
    "p99" : 1193.0651692700017,
    "p98" : 1087.01796402,
    "mean_rate" : 237.7918254154699,
    "p50" : 516.3878119999999,
    "p999" : 1504.6817440890006
  },
  "owl.IPASetCounterPerf.size_latency" : {
    "rate_units" : "calls/second",
    "count" : 20221,
    "duration_units" : "milliseconds",
    "p75" : 658.8595725,
    "m1_rate" : 238.82684160037113,
    "mean" : 503.6615976293774,
    "min" : 66.516111,
    "p95" : 939.9183561999998,
    "m15_rate" : 19.30530791661953,
    "max" : 1569.881967,
    "stddev" : 255.37817571616878,
    "m5_rate" : 56.29018332984392,
    "p99" : 1236.040919240001,
    "p98" : 1156.3254085999997,
    "mean_rate" : 94.78730670545936,
    "p50" : 512.6369955,
    "p999" : 1568.876482886
  }
}
{
  "owl.IPASetPlainPerf.add_latency" : {
    "rate_units" : "calls/second",
    "count" : 30605,
    "duration_units" : "milliseconds",
    "p75" : 1313.125077,
    "m1_rate" : 342.43103110380446,
    "mean" : 1040.5221065321011,
    "min" : 69.081097,
    "p95" : 1656.79930825,
    "m15_rate" : 33.05038839282489,
    "max" : 3257.3975,
    "stddev" : 457.9959654346295,
    "m5_rate" : 93.73699522298121,
    "p99" : 2359.27881951,
    "p98" : 2179.47175044,
    "mean_rate" : 98.64644638786903,
    "p50" : 1045.6840009999999,
    "p999" : 3249.9398254030007
  },
  "owl.IPASetPlainPerf.cass_op_latency" : {
    "rate_units" : "calls/second",
    "count" : 101101,
    "duration_units" : "milliseconds",
    "p75" : 1341.90049875,
    "m1_rate" : 2097.7225439738377,
    "mean" : 1072.091284970817,
    "min" : 76.14981999999999,
    "p95" : 1720.2057634499997,
    "m15_rate" : 2265.399467481432,
    "max" : 3251.4420729999997,
    "stddev" : 454.60006889956554,
    "m5_rate" : 2233.7845388111946,
    "p99" : 2574.0182805500003,
    "p98" : 2167.2648783799996,
    "mean_rate" : 1975.3934769709588,
    "p50" : 1056.570995,
    "p999" : 3244.396000884001
  },
  "owl.IPASetPlainPerf.contains_latency" : {
    "rate_units" : "calls/second",
    "count" : 50302,
    "duration_units" : "milliseconds",
    "p75" : 1346.748435,
    "m1_rate" : 563.0352520926996,
    "mean" : 1062.6696959591438,
    "min" : 74.47813599999999,
    "p95" : 1785.7527761499994,
    "m15_rate" : 54.3226929664875,
    "max" : 3263.203297,
    "stddev" : 467.8132051195297,
    "m5_rate" : 154.07755332412387,
    "p99" : 2504.7917149000004,
    "p98" : 2158.7502012399996,
    "mean_rate" : 162.13296227555006,
    "p50" : 1057.4521835,
    "p999" : 3263.003292236
  },
  "owl.IPASetPlainPerf.size_latency" : {
    "rate_units" : "calls/second",
    "count" : 20194,
    "duration_units" : "milliseconds",
    "p75" : 1439.10011575,
    "m1_rate" : 226.33807447675963,
    "mean" : 1206.1225604883268,
    "min" : 76.709751,
    "p95" : 2152.9320809499995,
    "m15_rate" : 21.80923086425674,
    "max" : 3314.148554,
    "stddev" : 502.6061757698076,
    "m5_rate" : 61.87052875292213,
    "p99" : 2986.555780980002,
    "p98" : 2568.3817418199997,
    "mean_rate" : 65.08890703689569,
    "p50" : 1172.812307,
    "p999" : 3312.872099019
  }
}
~~~
