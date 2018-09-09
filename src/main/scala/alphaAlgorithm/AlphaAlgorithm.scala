package alphaAlgorithm

import misc.{FullPairsInfoMap, PairInfo, PairNotation}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import relations.{FindFollowRelation, FindLogRelations}

object AlphaAlgorithm {

  implicit def mapPairEncoder: org.apache.spark.sql.Encoder[Map[String, (PairNotation, PairNotation)]] = org.apache.spark.sql.Encoders.kryo[Map[String, (PairNotation, PairNotation)]]
  implicit def pairInfoEncoder: org.apache.spark.sql.Encoder[PairInfo] = org.apache.spark.sql.Encoders.kryo[PairInfo]
  implicit def pairInfoListEncoder: org.apache.spark.sql.Encoder[List[PairInfo]] = org.apache.spark.sql.Encoders.kryo[List[PairInfo]]
  implicit def pairsMapEncoder: org.apache.spark.sql.Encoder[FullPairsInfoMap] = org.apache.spark.sql.Encoders.kryo[FullPairsInfoMap]
  implicit def pairInfoTuple2Encoder: org.apache.spark.sql.Encoder[(PairNotation,PairNotation)] = org.apache.spark.sql.Encoders.kryo[(PairNotation,PairNotation)]
  implicit def setPairNotationEncoder: org.apache.spark.sql.Encoder[Set[PairNotation]] = org.apache.spark.sql.Encoders.kryo[Set[PairNotation]]
  implicit def tuple2[A1, A2](
                               implicit e1: Encoder[A1],
                               e2: Encoder[A2]
                             ): Encoder[(A1,A2)] = Encoders.tuple[A1,A2](e1, e2)

  def parseLine(line: String) = {
    val fields = line.split(" ")
    val caseId = fields.head
    val trace = fields.tail.toList
    (caseId, trace)
  }

  def main(args: Array[String]): Unit = {
    val followRelation: FindFollowRelation = new FindFollowRelation()
    val findLogRelations: FindLogRelations = new FindLogRelations()

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

    /**
      * pairInfo is in the following form
      * AB,PairNotation(DIRECT, FOLLOW)
      * AB,PairNotation(INVERSE, FOLLOW)
      */
    val pairInfo = tracesDS
      .map(traces => followRelation.findFollowRelation(traces))
      .map(x=>x.getPairsMap())
      .flatMap(map=>map.toSeq)  //map to collection of tuples
      .map(x=> List(new PairInfo((x._1, new PairNotation(x._2._1.pairNotation))), new PairInfo((x._1, new PairNotation(x._2._2.pairNotation)))))
      .flatMap(x=>x.toSeq)

    pairInfo.cache()
    pairInfo.foreach(x=>println(x.toString))
    /**
      * relations in  the following form
      * (FB,CAUSALITY)
      * (BB,NEVER_FOLLOW)
      * (AB,PARALLELISM)
      */
    val relations = pairInfo
      .groupByKey(x=> x.getPairName())
      .mapGroups{case(k, iter) => (k, iter.map(x => x.getPairNotation()).toSet)}    //TODO unique objects must be inserted
      .map(x=>findLogRelations.findRelations(x))

    relations.cache()

    relations.foreach(x=>println(x.toString))

    // Stop the session
    spark.stop()
  }

}
