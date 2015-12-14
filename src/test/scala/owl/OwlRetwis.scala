package owl

import com.websudos.phantom.connectors.KeySpace

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class OwlRetwis extends OwlTest {
  override implicit val space = KeySpace("owl_retwis")

  val nUsers = config.getInt("retwis.users")
  val avgFollowers = config.getInt("retwis.followers")
  val zipf = config.getDouble("retwis.zipf")

  "OwlRetwis" should "initialize social graph" in {
    Await.result(service.initSocialGraph(nUsers, avgFollowers, zipf), Duration.Inf)
    println(s"-- social graph initialized ($nUsers users, ${nUsers*avgFollowers} follows)")

    // spot checks
    val u1 = service.userUUID(1)
    service.getUserById(u1).futureValue shouldBe defined
    val u1nfollowers = service.followersOf(u1).futureValue.length
    println(s"-- user[1] num followers = $u1nfollowers")
    u1nfollowers should be > avgFollowers/2
  }

  it should "initialize tweets" in {

  }

}
