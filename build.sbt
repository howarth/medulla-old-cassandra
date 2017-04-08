name := "medulla"
version := "0.1.0"
scalaVersion := "2.11.8"
libraryDependencies += "org.yaml" % "snakeyaml" % "1.8"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"
libraryDependencies +=  "com.typesafe.play" %% "play" % "2.5.12"


val phantomVersion = "2.6.1"
libraryDependencies += "com.outworkers"  %% "phantom-dsl" % phantomVersion
libraryDependencies += "com.outworkers"  %% "phantom-connectors" % phantomVersion

libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.6.4"




fork in console := true
javaOptions in console ++= Seq(
        "-Xms256M", "-Xmx3G", "-XX:MaxPermSize=1024M", "-XX:+UseConcMarkSweepGC")
