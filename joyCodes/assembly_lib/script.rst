sbt
set scalaVersion := "2.10.4"
set libraryDependencies += balabala
console ?
assembly

.history:

"org.json4s" % "json4s-jackson_2.10" % "3.3.0"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)


