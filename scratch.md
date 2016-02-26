# Scratch notes

### Manual commands to paste into Scala console
~~~scala
import owl._
import Util._
import com.websudos.phantom.dsl._
object ReplService extends { override implicit val space = KeySpace("repl") } with OwlService
import ReplService._

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

~~~

### SQL commands
~~~sql
CREATE KEYSPACE IF NOT EXISTS owl WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
CREATE TABLE users (id int PRIMARY KEY, username text, name text);
INSERT INTO users(id,username,name) VALUES (42, 'tealover42', 'Arthur Dent');
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
