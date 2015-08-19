// import sbt.Keys._

// factor out common settings into a sequence

name := "Scala Analytics"

version := "1.0"

// set the Scala version used for the project
scalaVersion := "2.11.7"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

libraryDependencies  ++= Seq(
  "sh.den" % "scala-offheap_2.11" % "0.1-SNAPSHOT",
  "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"
)

resolvers ++= Seq(
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)

javaOptions += "-Xms2g -Xmx2g"

fork in test := true

javaOptions in test ++= Seq("-Xms2G", "-Xmx2G", "-XX:MaxPermSize=1024M", "-XX:+UseConcMarkSweepGC")
