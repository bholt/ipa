package owl

import com.websudos.phantom.dsl._
import ipa.apps.retwis.Retwis
import owl.Util._

import scala.concurrent.duration._
import scala.concurrent.Await

class RetwisTest extends {
  override implicit val space = KeySpace("owl_retwis")
} with OwlTest with Retwis {

  it should "initialize social graph" in {

    println(">>> before init")
    generate()
    println(">>> after init")

    println(">>> checking if social graph was initialized")
    // spot checks
    val u1 = User.id(1)
    getUserById(u1).futureValue shouldBe defined
    val u1nfollowers = followersOf(u1).futureValue.length
    println(s"-- user[1] num followers = $u1nfollowers")
    u1nfollowers should be > config.avgFollowers/2
  }

  it should "initialize tweets" in {

    val fs = followersOf(User.id(1))
        .flatMap { fs =>
          fs.take(5).map(f => timeline(user = f, limit = 1)).bundle
        }
        .map { _.flatten }


    val ts = Await.result(fs, Duration.Inf).toSeq

    println(s"Tweets:\n- ${ts.mkString("\n- ")}")
    ts.length should be >= 1
    // ts.toSet.size should be (1) // tweets not guaranteed to be in order
  }

  it should "run each work once" in {
    println("### NewUser")
    Tasks.NewUser()

    println("### Follow")
    Tasks.Follow()

    println("### Unfollow")
    Tasks.Unfollow()

    println("### Tweet")
    Tasks.Tweet()

    println("### Timeline")
    Tasks.Timeline()

  }

   it should "run workload" in {
     if (config.disable_perf_tests) {
       println(">>> skipping performance tests")
     } else {
       workload()
     }
   }
}
