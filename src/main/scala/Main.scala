import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, VertexId}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.graphframes._
import org.apache.spark.graphx._


object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val sparkMasterConf = "local"

    val spark = SparkSession
      .builder()
      .appName("Main")
      .config("spark.master", sparkMasterConf)
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("OFF")

    val dataReader = new DataReader()
    val edgesPath = args(0) // path to wiki-topcats.txt or simplified version
    val verticesPath = args(1) // path to wiki-topcats-page-names.txt or simplified version
    val edges: DataFrame = dataReader.read_edges(edgesPath, spark)
    val vertices: DataFrame = dataReader.read_vertices(verticesPath, spark)
    val graph = GraphFrame(vertices, edges)
    graph.labelPropagation
      .maxIter(100).run()
      .write.csv("output")
  }
}
