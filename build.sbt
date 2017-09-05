organization := "org.scorexfoundation"

name := "avl-iodb"

version := "0.2.6"

scalaVersion := "2.12.3"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/releases"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
  "ch.qos.logback" % "logback-classic" % "1.+" % "test",
  "org.scorexfoundation" %% "scrypto" % "2.+",
  "org.scorexfoundation" %% "iodb" % "0.+",
  "com.storm-enroute" %% "scalameter" % "0.8.2" % "bench"
)

testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")

testOptions in Test := Seq(Tests.Filter(t => !t.matches(".*Benchmark$")))

parallelExecution in Test := false

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

pomExtra :=
  <scm>
    <url>git@github.com:ScorexFoundation/AVLIODB.git</url>
    <connection>scm:git:git@github.com:ScorexFoundation/AVLIODB.git</connection>
  </scm>
    <developers>
      <developer>
        <id>kushti</id>
        <name>Alexander Chepurnoy</name>
        <url>http://chepurnoy.org/</url>
      </developer>
    </developers>

lazy val Benchmark = config("bench") extend Test

lazy val avliodb = (project in file(".")).configs(Benchmark).settings(inConfig(Benchmark)(Defaults.testSettings): _*)
