import owl._
import Util._
import com.websudos.phantom.dsl._
object ReplService extends OwlService
import ReplService._

val arthur = User(username = "tealuver", name = "Arthur Dent")
val ford = User(username = "hastowel", name = "Ford Prefect")
val zaphod = User(username = "froodyprez", name = "Zaphod Beeblebrox")

val t1 = Tweet(user = arthur.id, body = "Nutri-matic drinks are the worst. #fml")
val t2 = Tweet(user = zaphod.id, body = "Things more important than my ego: none")

await(service.createTables)
await(Future.sequence(List(arthur, ford, zaphod) map (service.store(_))))

await(service.follow(arthur.id, zaphod.id))
await(service.follow(ford.id, zaphod.id))
await(service.follow(ford.id, arthur.id))

await(service.post(t1))
await(service.post(t2))
