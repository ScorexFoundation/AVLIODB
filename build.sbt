organization := "org.scorexfoundation"

name := "avl-iodb"

version := "0.2.0"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
  "ch.qos.logback" % "logback-classic" % "1.+" % "test",
  "org.scorexfoundation" %% "scrypto" % "1.2.3-SNAPSHOT" exclude("org.slf4j", "slf4j-api"),
  "org.scorexfoundation" %% "iodb" % "0.3.1"
)

scalacOptions ++= Seq("-Xdisable-assertions")

licenses := Seq("CC0" -> url("https://creativecommons.org/publicdomain/zero/1.0/legalcode"))

homepage := Some(url("https://github.com/ScorexFoundation/AVLIODB"))

publishMavenStyle := true

publishArtifact in Test := false

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomIncludeRepository := { _ => false }