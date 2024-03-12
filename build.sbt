import Dependencies._
import Settings._

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.4.0"

lazy val root = (project in file("."))
  .settings(
    name := "alpaca_watcher",
    idePackagePrefix := Some("org.cardinal.alpaca_watcher")
  )

val AkkaVersion = "2.9.1"

root.resolvers += "Akka library repository".at("https://repo.akka.io/maven")

libraryDependencies ++= Seq(
  "org.scala-lang" %% "scala3-compiler" % "3.1.1",
  "com.cynance" %% "alpaca-scala" % "3.0.0",
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test
)
