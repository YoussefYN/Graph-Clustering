import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataReader {

  def read_edges(path: String, spark: SparkSession): DataFrame = {
    spark.read.format("csv")
      .option("header", "false")
      .option("sep", " ")
      .schema(StructType(Array(
      StructField("src", IntegerType, false),
      StructField("dst", IntegerType, false)
    )))
      .load(path)
      .limit(1500000)
  }

  def read_vertices(path: String, spark: SparkSession): DataFrame = {
    spark.read.format("csv")
      .option("header", "false")
      .option("sep", " ")
      .schema(StructType(Array(
        StructField("id", IntegerType, false),
        StructField("node_name", StringType, false)
      )))
      .load(path)
  }
}
