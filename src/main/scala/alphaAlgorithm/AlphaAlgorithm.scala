package alphaAlgorithm

import misc.{CausalGroup, FullPairsInfoMap, Pair, PairInfo, PairNotation, Relation}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import relations.{FindCausalGroups, FindFollowRelation, FindLogRelations}
import tools.TraceTools

//TODO event must be a generic, not only String type
//TODO refactor main AlphaAlgorithm
//TODO check for a better implementation of encoders
object AlphaAlgorithm {

  implicit def mapPairEncoder: org.apache.spark.sql.Encoder[Map[String, (PairNotation, PairNotation)]] = org.apache.spark.sql.Encoders.kryo[Map[String, (PairNotation, PairNotation)]]
  implicit def pairInfoEncoder: org.apache.spark.sql.Encoder[PairInfo] = org.apache.spark.sql.Encoders.kryo[PairInfo]
  implicit def pairInfoListEncoder: org.apache.spark.sql.Encoder[List[PairInfo]] = org.apache.spark.sql.Encoders.kryo[List[PairInfo]]
  implicit def pairsMapEncoder: org.apache.spark.sql.Encoder[FullPairsInfoMap] = org.apache.spark.sql.Encoders.kryo[FullPairsInfoMap]
  implicit def pairInfoTuple2Encoder: org.apache.spark.sql.Encoder[(PairNotation,PairNotation)] = org.apache.spark.sql.Encoders.kryo[(PairNotation,PairNotation)]
  implicit def setOfPairNotationEncoder: org.apache.spark.sql.Encoder[Set[PairNotation]] = org.apache.spark.sql.Encoders.kryo[Set[PairNotation]]
  implicit def pairEncoder: org.apache.spark.sql.Encoder[Pair] = org.apache.spark.sql.Encoders.kryo[Pair]
  implicit def causalGroupEncoder: org.apache.spark.sql.Encoder[CausalGroup[String]] = org.apache.spark.sql.Encoders.kryo[CausalGroup[String]]
  implicit def tuple2[A1, A2](
                               implicit e1: Encoder[A1],
                               e2: Encoder[A2]
                             ): Encoder[(A1,A2)] = Encoders.tuple[A1,A2](e1, e2)

  def main(args: Array[String]): Unit = {
    val followRelation: FindFollowRelation = new FindFollowRelation()
    val findLogRelations: FindLogRelations = new FindLogRelations()
    val traceTools: TraceTools = new TraceTools()

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .appName("AlphaAlgorithm")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    //traces like (case1, List(A,B,C,D))
    val traces = spark.sparkContext
      .textFile("src/main/resources/log1.txt")
      .map(x=>traceTools.parseLine(x))

    // Convert to a DataSet
    import spark.implicits._
    val tracesDS = traces.toDS()

    //Sorted list of all event types
    val events = tracesDS
        .map(x=>x._2)
        .flatMap(x=>x.toSet)
        .collect()
        .toSet
        .toList
        .sorted

    //construct a list of pair events for which computations must be made
    val pairsToExamine = traceTools.constructPairsForComputationFromEvents(events)

    /**
      * pairInfo is in the following form
      * AB,PairNotation(DIRECT, FOLLOW)
      * AB,PairNotation(INVERSE, FOLLOW)
      */
    val pairInfo = tracesDS
      .map(traces => followRelation.findFollowRelation(traces, pairsToExamine))
      .map(x=>x.getPairsMap())
      .flatMap(map=>map.toSeq)  //map to collection of tuples
      .map(x=> List(new PairInfo((x._1, new PairNotation(x._2._1.pairNotation))), new PairInfo((x._1, new PairNotation(x._2._2.pairNotation)))))
      .flatMap(x=>x.toSeq)

    pairInfo.cache()

    /**
      * relations in  the following form, Footprint graph
      * (FB,CAUSALITY)
      * (BB,NEVER_FOLLOW)
      * (AB,PARALLELISM)
      */
    val logRelations = pairInfo
      .groupByKey(x=> x.getPairName())
      .mapGroups{case(k, iter) => (new Pair(k.charAt(0).toString, k.charAt(1).toString), iter.map(x => x.getPairNotation()).toSet)}    //TODO unique objects must be inserted
      .map(x=>findLogRelations.findFootPrintGraph(x))

    logRelations.cache()

    logRelations.foreach(x=>println(x.toString))

    //compute causal groups - Step 4
    //directCausalGroups are all causality relations because they are by default causal group
    val findCausalGroups: FindCausalGroups = new FindCausalGroups(logRelations) //String is event type
    val causalGroups = findCausalGroups.extractCausalGroups()

    // Stop the session
    spark.stop()
  }

}
