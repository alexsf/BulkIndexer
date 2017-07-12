name := "bulk-indexer"

version := "1.0"

scalaVersion := "2.11.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1" % "provided"
libraryDependencies += "org.gnieh" % "diffson-circe_2.11" % "2.2.1"
libraryDependencies += "org.scalaj" % "scalaj-http_2.11" % "2.3.0"


mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}
