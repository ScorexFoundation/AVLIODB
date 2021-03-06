import sbt.Keys.testFrameworks

name := "avl-iodb"

lazy val commonSettings = Seq(
  organization := "org.scorexfoundation",
  version := "0.2.15",
  scalaVersion := "2.12.4",
  licenses := Seq("CC0" -> url("https://creativecommons.org/publicdomain/zero/1.0/legalcode")),
  homepage := Some(url("https://github.com/ScorexFoundation/AVLIODB")),
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

)

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/releases"

libraryDependencies ++= Seq(
  "javax.xml.bind" % "jaxb-api" % "2.+",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.scorexfoundation" %% "scrypto" % "2.1.4",
  "org.scorexfoundation" %% "iodb" % "0.3.2"
)

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.+" % "test",
  "org.scalacheck" %% "scalacheck" % "1.13.+" % "test",
  "com.storm-enroute" %% "scalameter" % "0.9" % "test"
)

testOptions in Test := Seq(Tests.Filter(t => !t.matches(".*Benchmark$")))

scalacOptions ++= Seq("-Xdisable-assertions")

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

lazy val avliodb = (project in file(".")).settings(commonSettings: _*)

lazy val benchmarks = (project in file("benchmarks"))
  .settings(
    commonSettings,
    name := "scrypto-benchmarks",
    libraryDependencies ++= Seq(
      "com.storm-enroute" %% "scalameter" % "0.9" % "test"
    ),
    publishArtifact := false,
    resolvers ++= Seq("Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases"),
    testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),
    parallelExecution in Test := false,
    logBuffered := false
  )
  .dependsOn(avliodb)
  .enablePlugins(JmhPlugin)

