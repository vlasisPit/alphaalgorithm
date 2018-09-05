package alphaAlgorithm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object AlphaAlgorithm {

  def parseLine(line: String) = {
    val fields = line.split(" ")
    val caseId = fields.head
    val trace = fields.tail.toList
    (caseId, trace)
  }

  /**
    * Find follow relation in the form of (PairAB, directAccess, InverseAccess)
    * where directAccess is true when B follows A (AB) in the trace
    * and InverseAccess is true when A follows B (BA) in the trace
    * @param traceTuple
    * @return
    */
  def findFollowRelation(traceTuple: (String, List[String])): List[(String, Boolean, Boolean)] = {
    var pairInfo = new ListBuffer[(String, Boolean, Boolean)]()
    for( i <- 0 to traceTuple._2.length-2){
      val tuple3 = (traceTuple._2(i)+traceTuple._2(i+1),true,false)
      pairInfo = pairInfo +=  tuple3
    }
    return pairInfo.toList
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

    val tracesTuple = tracesDS
          .map(findFollowRelation)
          .flatMap(x=>x.toSeq)

    tracesTuple.foreach(x=>println(x))
  }

}
