name := "Graph-Clustering"

version := "0.1"

scalaVersion := "2.11.11"
val sparkVersion = "2.4.0"

resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "graphframes" % "graphframes" % "0.7.0-spark2.4-s_2.11"
)