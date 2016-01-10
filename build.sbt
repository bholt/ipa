name := "owl"

version := "0.1"

scalaVersion := "2.11.7"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

// fork for these so we can explicitly `exit` and kill all our threads
fork in run := true

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

  val phantomV = "1.17.7"

  Seq(
    "com.websudos"        %% "phantom-dsl"                % phantomV,
    "com.websudos"        %% "phantom-testkit"            % phantomV,
    "com.websudos"        %% "phantom-connectors"         % phantomV,
    "org.scalatest"       %% "scalatest"                  % "2.2.4"     % "test",
    "com.typesafe"        %  "config"                     % "1.3.0",
    "org.apache.commons"  %  "commons-math3"              % "3.5",
    "nl.grons"            %% "metrics-scala"              % "3.5.2"
  )
}
