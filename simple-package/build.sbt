// import sbt._
// import sbt.Keys._
// import java.io.File
// import AssemblyKeys._

name := "spark-tools" + "-1.8.0"

scalaVersion := "2.11.8"

version      := "0.1.0-SNAPSHOT"

 
// resolvers += "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
 
// resolvers += "Typesafe" at "https://repo.typesafe.com/typesafe/releases/"
 
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-mllib_2.11" % "2.1.0" % Provided
  // "org.apache.spark" %% "spark-core" % "2.0.2" % Provided
  // ("com.github.nearbydelta" %% "deepspark" % "1.2.0" )
  // ("org.json4s" % "json4s-jackson_2.10" % "3.3.0" )
)
// libraryDependencies += "com.github.etaty" % "rediscala_2.11" % "1.8.0"
//libraryDependencies += "com.sun.jersey" % "jersey-bundle" % "1.17.1"




// libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-paranamer" % "2.9.1"
// libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.1"
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.1.0"


// libraryDependencies += "com.github.nearbydelta" %% "deepspark" % "1.2.0"
// libraryDependencies += "org.json4s" % "json4s-jackson_2.10" % "3.3.0"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
// val libraryDependencies = "org.json4s" % "json4s-jackson_2.10" % "3.3.0" % "provided"
// libraryDependencied ++= Seq(
//   ("org.json4s" % "json4s-jackson_2.12.0-M4" % "3.3.0" % "provided").
//     exclude("org.scala-lang", "scala-library")
// )

// libraryDependencies ++= Seq(
//   ("org.json4s" % "json4s-jackson_2.12.0-M4" % "3.3.0" % "provided").
//     exclude("org.scala-lang", "scala-library")
// )

// resolvers += "Apache repo" at "https://repository.apache.org/content/repositories/releases"

// assemblySettings

// mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
//     case x => val oldStrategy = old(x) if (oldStrategy == MergeStrategy.deduplicate) MergeStrategy.discard else oldStrategy
//   }
// }
