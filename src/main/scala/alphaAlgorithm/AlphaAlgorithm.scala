package alphaAlgorithm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object AlphaAlgorithm {

  def parseLine(line: String) = {
    val fields = line.split(" ")
    val caseId = fields.head
    val trace = fields.tail
    (caseId, trace)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .appName("AlphaAlgorithm")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    val traces = spark.sparkContext
      .textFile("src/main/resources/log.txt")
      .map(parseLine)

    // Convert to a DataSet
    import spark.implicits._
    val tracesDS = traces.toDS()

    tracesDS.show()
  }
}
