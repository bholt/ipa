name := "owl"

version := "0.1"

scalaVersion := "2.11.7"

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-feature",
  "-language:postfixOps",
  "-language:implicitConversions"
)

// lazy val reservations = project.in(file("reservations"))

// allow packaging using docker
enablePlugins(DockerPlugin)
enablePlugins(JavaAppPackaging)

dockerRepository := Some("bholt")
version in Docker := "latest"

mainClass in Compile := Some("owl.All") // set main for docker

import com.typesafe.sbt.packager.docker._
// dockerCommands += Cmd("ENV", "CASSANDRA_HOST", "cassandra")

dockerBaseImage := "bholt/cassandra:2.2.4"

// Allow overriding default command by rewriting dockerCommands.
// make 'ENTRYPOINT' an (optional) CMD instead, remove (empty) CMD, & keep the rest
dockerCommands := dockerCommands.value.flatMap {
  case ExecCmd("ENTRYPOINT", args @ _*) => Some(ExecCmd("CMD", args:_*))
  case ExecCmd("CMD", _*) => None
  case Cmd("USER", _) => None
  case other => Some(other)
}

// fork for these so we can explicitly `exit` and kill all our threads
fork in run := true

parallelExecution in Test := false

// pass all System properties to forked JVM
import scala.collection.JavaConversions._
javaOptions in run ++= {
  val props = System.getProperties
  props.stringPropertyNames
       .map { k => s"-D$k=${props.getProperty(k)}" }
       .toSeq
}

// send output to the build's standard output and error (when forked)
outputStrategy := Some(StdoutOutput)

// set main class for 'sbt run'
mainClass in (Compile, run) := Some("owl.All")

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
  val phantomV = "1.21.5"
  val finagleV = "6.33.0"
  Seq(
    "com.websudos"        %% "phantom-dsl"                % phantomV,
    "com.websudos"        %% "phantom-connectors"         % phantomV,
    "org.scalatest"       %% "scalatest"                  % "2.2.4"     % "test",
    "com.typesafe"        %  "config"                     % "1.3.0",
    "org.apache.commons"  %  "commons-math3"              % "3.5",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.3",
    "com.codahale.metrics"       % "metrics-json"         % "3.0.2",
    "com.twitter" %% "finagle-core" % finagleV,
    "com.twitter" %% "finagle-thrift" % finagleV,
    "com.twitter" %% "finagle-thriftmux" % finagleV,
    "com.twitter" %% "scrooge-core" % "4.5.0"
  )
}

// better repl (sbt owl/test:console)
libraryDependencies += "com.lihaoyi" % "ammonite-repl" % "0.5.1" % "test" cross CrossVersion.full
initialCommands in (Test, console) := """ammonite.repl.Main.run("import scala.concurrent._; import scala.concurrent.duration._")"""