name := "ipa-reservations"

version := "0.1"

scalaVersion := "2.11.7"

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-feature",
  "-language:postfixOps"
)

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  Resolver.bintrayRepo("websudos", "oss-releases"),
  "spray repo"                       at "http://repo.spray.io",
  "Typesafe repository snapshots"    at "http://repo.typesafe.com/typesafe/snapshots/",
  "Typesafe repository releases"     at "http://repo.typesafe.com/typesafe/releases/",
  "Sonatype repo"                    at "https://oss.sonatype.org/content/groups/scala-tools/",
  "Sonatype releases"                at "https://oss.sonatype.org/content/repositories/releases",
  "Sonatype snapshots"               at "https://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype staging"                 at "http://oss.sonatype.org/content/repositories/staging",
  "Java.net Maven2 Repository"       at "http://download.java.net/maven/2/",
  "Twitter Repository"               at "http://maven.twttr.com"
)


libraryDependencies ++= {
  Seq(
//    "com.twitter" %% "finagle-core" % "6.33.0",
    "io.github.finagle" %% "finagle-serial-scodec" % "0.1.0-SNAPSHOT",
    "org.apache.cassandra" % "cassandra-all" % "2.2.4"
  )
}
