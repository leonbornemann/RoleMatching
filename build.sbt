name := "DatasetVersioning"

version := "0.1"

scalaVersion := "2.13.2"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

// https://mvnrepository.com/artifact/org.json4s/json4s-jackson
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.7.0-M4"
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.5"
// https://mvnrepository.com/artifact/com.typesafe.scala-logging/scala-logging
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
// https://mvnrepository.com/artifact/org.apache.commons/commons-math3
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.2"
// https://mvnrepository.com/artifact/org.apache.commons/commons-csv
libraryDependencies += "org.apache.commons" % "commons-csv" % "1.8"
// https://mvnrepository.com/artifact/org.json/json
libraryDependencies += "org.json" % "json" % "20200518"
// https://mvnrepository.com/artifact/org.scala-graph/graph-core
libraryDependencies += "org.scala-graph" %% "graph-core" % "1.13.2"
// https://mvnrepository.com/artifact/org.apache.commons/commons-compress
//libraryDependencies += "org.apache.commons" % "commons-compress" % "1.20"
// https://mvnrepository.com/artifact/org.tukaani/xz
//libraryDependencies += "org.tukaani" % "xz" % "1.9"
// https://mvnrepository.com/artifact/org.json4s/json4s-ext
libraryDependencies += "org.json4s" %% "json4s-ext" % "3.7.0-M4"
// https://mvnrepository.com/artifact/org.jgrapht/jgrapht-core
libraryDependencies += "org.jgrapht" % "jgrapht-core" % "1.4.0"


