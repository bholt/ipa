# Scratch notes

### Manual commands to paste into Scala console
~~~scala
import owl._
import ipa._
import Util._
import com.websudos.phantom.dsl._
object ReplService extends { override implicit val space = KeySpace("repl") } with OwlService
import ReplService._

implicit val space = KeySpace("tickets"); implicit val imps = CommonImplicits()
val tickets = new BoundedCounter("tickets")

implicit val space = KeySpace("reservations")
implicit val imps = CommonImplicits()

implicit val space = KeySpace("bc_tests"); implicit val imps = CommonImplicits()
val bc = new BoundedCounter("bc")


import com.twitter.util.{Future => TwFuture, Await => TwAwait}
implicit class TwFutureValue[T](f: TwFuture[T]) { def fv(): T = TwAwait.result(f) }

val arthur = User(username = "tealuver", name = "Arthur Dent")
val ford = User(username = "hastowel", name = "Ford Prefect")
val zaphod = User(username = "froodyprez", name = "Zaphod Beeblebrox")

val t1 = Tweet(user = arthur.id, body = "Nutri-matic drinks are the worst. #fml")
val t2 = Tweet(user = zaphod.id, body = "Things more important than my ego: none")

service.resetKeyspace()

implicit val consistency = ConsistencyLevel.ALL

Seq(arthur, ford, zaphod).map(service.store).bundle.await

await(service.follow(arthur.id, zaphod.id))
await(service.follow(ford.id, zaphod.id))
await(service.follow(ford.id, arthur.id))

await(service.post(t1))
await(service.post(t2))

import java.util.concurrent.TimeUnit
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.ObjectMapper
import com.codahale.metrics.json.MetricsModule
import scala.collection.JavaConversions._

val mapper = new ObjectMapper().registerModule(new MetricsModule(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, false)).registerModule(DefaultScalaModule)


import owl._
import ipa._
import Util._
import com.websudos.phantom.dsl._
object ReplService extends { override implicit val space = KeySpace("repl") } with OwlService
import ReplService._

implicit val space = KeySpace("bc_tests"); implicit val imps = CommonImplicits()
val bc = new BoundedCounter("bc")
val st = bc.init(1.id, 0).await()

~~~

### SQL commands
~~~sql
CREATE KEYSPACE IF NOT EXISTS owl WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
CREATE TABLE users (id int PRIMARY KEY, username text, name text);
INSERT INTO users(id,username,name) VALUES (42, 'tealover42', 'Arthur Dent');

CREATE OR REPLACE FUNCTION alloc_total (alloc map<int,bigint>) RETURNS NULL ON NULL INPUT RETURNS bigint LANGUAGE java AS '
  long total = 0;
  for (Object e : alloc.values()) total += (Long)e;
  return total;
';

CREATE OR REPLACE FUNCTION alloc_total (alloc map<int,bigint>) RETURNS NULL ON NULL INPUT RETURNS bigint LANGUAGE java AS 'long total = 0; for (Object e : alloc.values()) total += (Long)e; return total;';

CREATE OR REPLACE FUNCTION sum_values (alloc map<int,int>) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'int total = 0; for (Object e : alloc.values()) total += (Integer)e; return total;';

~~~

### Docker
~~~bash
# Launch cassandra cluster via blockade
> sudo blockade up

# Build docker image
> sudo sbt docker:publishLocal

# Run via docker image (and pass arguments!)
> sudo docker run -ti --link owl_c1:cassandra --rm bholt/owl -Dcassandra.replication.factor=3
~~~
