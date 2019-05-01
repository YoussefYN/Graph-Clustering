import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.graphframes._


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
    val edgesPath = args(0)
    val verticesPath = args(1)
    val edges: DataFrame = dataReader.read_edges(edgesPath, spark)
    val vertices: DataFrame = dataReader.read_vertices(verticesPath, spark)
    val graph = GraphFrame(vertices, edges).toGraphX.mapVertices((vid, _) => vid.toLong)

    val cluster = new GraphLabeling()
    cluster
      .run(graph, 100)
      .map(_.toString.drop(1).dropRight(1))
      .saveAsTextFile("outDir")
  }
}
