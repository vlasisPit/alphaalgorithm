package alphaAlgorithm

import misc.{PairInfo, PairNotation}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import relations.FindFollowRelation

object AlphaAlgorithm {

  def parseLine(line: String) = {
    val fields = line.split(" ")
    val caseId = fields.head
    val trace = fields.tail.toList
    (caseId, trace)
  }

  def main(args: Array[String]): Unit = {
    import encoders.DataEncoders._
    val followRelation: FindFollowRelation = new FindFollowRelation()

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

/*    val tracesTuple = tracesDS
          .map(traces => followRelation.findFollowRelation(traces))
          .map(x=>x.getPairsMap())
          .flatMap(map=>map.toSeq)  //map to collection of tuples
          .map(x=>x._1+","+x._2._1.pairNotation+","+x._2._2.pairNotation)*/

    val tracesTuple = tracesDS
          .map(traces => followRelation.findFollowRelation(traces))
          .map(x=>x.getPairsMap())
          .flatMap(map=>map.toSeq)  //map to collection of tuples
          .map(x=> List(new PairInfo((x._1, new PairNotation(x._2._1.pairNotation))), new PairInfo((x._1, new PairNotation(x._2._2.pairNotation)))))
          .flatMap(x=>x.toSeq)

    tracesTuple.foreach(x=>println(x.toString))
  }

}
