package alphaAlgorithm

import java.util

import misc.{CausalGroup, FullPairsInfoMap, Pair, PairInfo, PairNotation, Relation}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import relations.{FindCausalGroups, FindFollowRelation, FindLogRelations}
import tools.TraceTools

//TODO event must be a generic, not only String type
//TODO refactor main AlphaAlgorithm
object AlphaAlgorithm {

  implicit def mapPairEncoder: org.apache.spark.sql.Encoder[Map[String, (PairNotation, PairNotation)]] = org.apache.spark.sql.Encoders.kryo[Map[String, (PairNotation, PairNotation)]]
  implicit def pairInfoEncoder: org.apache.spark.sql.Encoder[PairInfo] = org.apache.spark.sql.Encoders.kryo[PairInfo]
  implicit def pairInfoListEncoder: org.apache.spark.sql.Encoder[List[PairInfo]] = org.apache.spark.sql.Encoders.kryo[List[PairInfo]]
  implicit def pairsMapEncoder: org.apache.spark.sql.Encoder[FullPairsInfoMap] = org.apache.spark.sql.Encoders.kryo[FullPairsInfoMap]
  implicit def pairInfoTuple2Encoder: org.apache.spark.sql.Encoder[(PairNotation,PairNotation)] = org.apache.spark.sql.Encoders.kryo[(PairNotation,PairNotation)]
  implicit def setOfPairNotationEncoder: org.apache.spark.sql.Encoder[Set[PairNotation]] = org.apache.spark.sql.Encoders.kryo[Set[PairNotation]]
  implicit def pairEncoder: org.apache.spark.sql.Encoder[Pair[String]] = org.apache.spark.sql.Encoders.kryo[Pair[String]]
  implicit def causalGroupEncoder: org.apache.spark.sql.Encoder[CausalGroup[String]] = org.apache.spark.sql.Encoders.kryo[CausalGroup[String]]
  //implicit def causalGroupSetEncoder: org.apache.spark.sql.Encoder[CausalGroup[Set[String]]] = org.apache.spark.sql.Encoders.kryo[CausalGroup[Set[String]]]
  implicit def tuple2[A1, A2](
                               implicit e1: Encoder[A1],
                               e2: Encoder[A2]
                             ): Encoder[(A1,A2)] = Encoders.tuple[A1,A2](e1, e2)

  def main(args: Array[String]): Unit = {
    val followRelation: FindFollowRelation = new FindFollowRelation()
    val findLogRelations: FindLogRelations = new FindLogRelations()
    val findCausalGroups: FindCausalGroups[String] = new FindCausalGroups() //String is event type
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
      .textFile("src/main/resources/log2.txt")
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
      .mapGroups{case(k, iter) => (k, iter.map(x => x.getPairNotation()).toSet)}    //TODO unique objects must be inserted
      .map(x=>findLogRelations.findFootPrintGraph(x))

    logRelations.cache()

    logRelations.foreach(x=>println(x.toString))

    //compute causal groups - Step 4
    //directCausalGroups are all causality relations because they are by default causal group
    val directCausalGroups = logRelations
        .filter(x=>x._2==Relation.CAUSALITY.toString)
        .map(x=>new Pair(x._1.charAt(0).toString, x._1.charAt(1).toString)) //TODO delete after refactoring for Pairs
        .map(x=>new CausalGroup(Set(x.member1), Set(x.member2)))

    directCausalGroups.foreach(x=>println(x.toString))

    //val causalGroups = findCausalGroups.enrichCausalGroups(directCausalGroups)

    val causalGroupsFromLeft = directCausalGroups
        .groupByKey(x=>x.getFirstGroup())
        .mapGroups{case(k, iter) => (k, iter.map(x => x.getSecondGroup().head).toSet)}
        .filter(x=>x._2.size>1)
        .map(x=>new CausalGroup(x._1, x._2))

    causalGroupsFromLeft.foreach(x=>println(x.toString))

    val causalGroupsFromRight = directCausalGroups
      .map(x=>new CausalGroup(x.getSecondGroup(), x.getFirstGroup()))
      .groupByKey(x=>x.getFirstGroup())
      .mapGroups{case(k, iter) => (k, iter.map(x => x.getSecondGroup().head).toSet)}
      .filter(x=>x._2.size>1)
      .map(x=>new CausalGroup(x._2, x._1))

    causalGroupsFromRight.foreach(x=>println(x.toString))

    val causalGroups : List[CausalGroup[String]]  = directCausalGroups.collect.toList


    // Stop the session
    spark.stop()
  }

}
