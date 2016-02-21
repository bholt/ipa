logLevel := Level.Warn

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.1.0-M3")

resolvers += "twitter-repo" at "https://maven.twttr.com"
addSbtPlugin("com.twitter" % "scrooge-sbt-plugin" % "4.5.0")
