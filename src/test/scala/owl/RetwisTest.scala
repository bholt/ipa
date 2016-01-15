package owl

import com.websudos.phantom.dsl._
import owl.Util._

class RetwisTest extends OwlTest with Retwis {

  it should "initialize social graph" in {

    println(">>> before init")
    generate()
    println(">>> after init")

    println(">>> checking if social graph was initialized")
    // spot checks
    val u1 = User.id(1)
    service.getUserById(u1).futureValue shouldBe defined
    val u1nfollowers = service.followersOf(u1).futureValue.length
    println(s"-- user[1] num followers = $u1nfollowers")
    u1nfollowers should be > avgFollowers/2
  }

  it should "initialize tweets" in {

    val ts = service.followersOf(User.id(1))
        .flatMap { fs =>
          fs.take(5).map(f => service.timeline(user = f, limit = 1)).bundle
        }
        .map { _.flatten }
        .futureValue
        .toSeq

    println(s"Tweets:\n- ${ts.mkString("\n- ")}")
    ts.length should be >= 1
    // ts.toSet.size should be (1) // tweets not guaranteed to be in order
  }

  it should "run each work once" in {
    import Workload.Tasks._
    println("### NewUser")
    NewUser()

    println("### Follow")
    Follow()

    println("### Unfollow")
    Unfollow()

    println("### Tweet")
    Tweet()

    println("### Timeline")
    Timeline()

  }

  it should "run workload" in {
    workload()
  }
}
