package steps

import misc.{CausalGroup, Pair, Relation}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

/**
  * Accept us input footprint's graph data. In the following form
  * (Pair(A, D),NEVER_FOLLOW)
  * (Pair(B, D),CAUSALITY)
  * (Pair(B, C),NEVER_FOLLOW)
  * (Pair(A, A),NEVER_FOLLOW)
  * (Pair(C, C),NEVER_FOLLOW)
  * (Pair(A, C),CAUSALITY)
  * (Pair(D, D),NEVER_FOLLOW)
  * (Pair(A, B),CAUSALITY)
  * (Pair(B, B),NEVER_FOLLOW)
  * (Pair(C, D),CAUSALITY)
  * Let Q,R be two sets of activities. Then (Q,R) is a causal group iff there is a causal relation -> from each element of Q
  * to each element of R (ie all pairwise combinations of elements of Q and R are in ->) and the members of Q and R are
  * not in ||
  *
  * The algorithm works as follows:
  * Lets say that we have the causal relations
  * a->d
  * a->c
  * b->d
  * b->c
  * and the following never follow relation
  * a#b
  * c#d
  * 1) Find all events from both sides (left and right) {a,b} and {c,d}
  * 2) Find all possible combination for each set (left and right) List[{a,b} {a} {b}] and List[{c,d} {c} {d}]
  * 3) Delete from above lists all the sets which are not in never follow relation with each other
  * 4) Connect all the sets (as causal groups) from two lists and keep only those for which all events are in causal relations
  */
@SerialVersionUID(100L)
class FindCausalGroups(val logRelations: Dataset[(Pair, String)]) extends Serializable {
  implicit def pairEncoder: org.apache.spark.sql.Encoder[Pair] = org.apache.spark.sql.Encoders.kryo[Pair]

  val spark = SparkSession.builder().getOrCreate()

  val never = logRelations
    .filter(x=>x._2==Relation.NEVER_FOLLOW.toString)
    .map(neverRelation => neverRelation._1)
    .distinct()
    .collect()
    .toList

  val neverBc = spark.sparkContext.broadcast(never)

  val causal = logRelations
    .filter(x=>x._2==Relation.CAUSALITY.toString)
    .map(neverRelation => neverRelation._1)
    .distinct()
    .collect()
    .toList

  val causalBc = spark.sparkContext.broadcast(causal)

  def extractCausalGroups():Dataset[CausalGroup[String]] = {
    implicit def stringEncoder: org.apache.spark.sql.Encoder[String] = org.apache.spark.sql.Encoders.kryo[String]
    implicit def tupleStringEncoder: org.apache.spark.sql.Encoder[List[(String, Set[String])]] = org.apache.spark.sql.Encoders.kryo[List[(String, Set[String])]]
    implicit def tupleListStringEncoder: org.apache.spark.sql.Encoder[(String, List[Set[String]])] = org.apache.spark.sql.Encoders.kryo[(String, List[Set[String]])]
    implicit def strEncoder: org.apache.spark.sql.Encoder[List[Set[String]]] = org.apache.spark.sql.Encoders.kryo[List[Set[String]]]
    implicit def gEncoder: org.apache.spark.sql.Encoder[(String, Set[String])] = org.apache.spark.sql.Encoders.kryo[(String, Set[String])]
    implicit def causalGroupGenericEncoder: org.apache.spark.sql.Encoder[CausalGroup[String]] = org.apache.spark.sql.Encoders.kryo[CausalGroup[String]]

    implicit def tuple2[A1, A2](
                                 implicit e1: Encoder[A1],
                                 e2: Encoder[A2]
                               ): Encoder[(A1,A2)] = Encoders.tuple[A1,A2](e1, e2)

    val directCausalRelations = logRelations
      .filter(x=>x._2==Relation.CAUSALITY.toString)
      .map(x=>x._1)
      .map(x=>(x.member1, x.member2))

    //1) Find all events from both sides (left and right) {a,b} and {c,d}
    val uniqueEventsFromLeftSideEvents = directCausalRelations
      .map(causal => ("left", causal._1))
      .groupByKey(x=>x._1)
      .mapGroups{case(k, iter) => (k, iter.map(x => x._2).toSet)}

    val uniqueEventsFromRightSideEvents = directCausalRelations
      .map(causal => ("right", causal._2))
      .groupByKey(x=>x._1)
      .mapGroups{case(k, iter) => (k, iter.map(x => x._2).toSet)}

    //2) Find all possible combination for each set (left and right) List[{a,b} {a} {b}] and List[{c,d} {c} {d}]
    //3) Delete from above lists all the sets which are not in never follow relation with each other
    val groups = uniqueEventsFromLeftSideEvents
      .union(uniqueEventsFromRightSideEvents)
      .flatMap(uniqueEvents => possibleSubsets(uniqueEvents))
      .filter(subset=> allRelationsAreNeverFollow(subset._2))
      .groupByKey(x=>x._1)
      .mapGroups{case(k, iter) => (k, iter.map(x => x._2).toList)}

    import spark.implicits._
    val leftGroups = groups.first()
    val rigthGroups = groups.orderBy($"value".desc).first()

    //4) Connect all the sets (as causal groups) from two lists and keep only those for which all events are in causal relations
    val causalGroups = leftGroups._2.toDS().as("left")
      .crossJoin(rigthGroups._2.toDS().as("right"))
      .where($"left.value" !== $"right.value")
      .toDF("left", "right")
      .filter(group=>isCausalRelationValid(group.getAs[Seq[String]]("left").toSet, group.getAs[Seq[String]]("right").toSet))
      .map(group=>new CausalGroup(group.getAs[Seq[String]]("left").toSet,group.getAs[Seq[String]]("right").toSet))

    causalGroups
  }

  def possibleSubsets(uniqueEvents: (String, Set[String])): List[(String, Set[String])] = {
    val possibleCombinations : PossibleCombinations = new PossibleCombinations(uniqueEvents._2.toList)
    val combinations : List[Set[String]] = possibleCombinations.extractAllPossibleCombinations()
    combinations
      .map(comb=>(uniqueEvents._1, comb))
  }

  /**
    * If there is at least one not-NeverFollow relation then the group must be removed
    * Example lets suppose possibleGroup= {b,c,e} and there is b||c, b#e and e#c.
    * Then the output must be {b,e} and {c,e}
    * @param possibleGroup
    * @return
    */
  def allRelationsAreNeverFollow(possibleGroup: Set[String]): Boolean = {
    val allPossiblePairs = for {
      (x, idxX) <- possibleGroup.zipWithIndex
      (y, idxY) <- possibleGroup.zipWithIndex
      if idxX < idxY
    } yield new Pair(x,y)
    val notNeverFollow = allPossiblePairs
      .find(x=> (!neverBc.value.contains(x) && !neverBc.value.contains(createInversePair(x))))

    notNeverFollow.isEmpty
  }

  def createInversePair(pair : Pair): Pair = {
    return new Pair(pair.getSecondMember(), pair.getFirstMember())
  }

  @Deprecated
  def computeCausalGroups(causalGroupFromLeftSide: List[Set[String]], causalGroupFromRightSide: List[Set[String]]): List[CausalGroup[String]] = {
    for {
      groupA <- causalGroupFromLeftSide
      groupB <- causalGroupFromRightSide
      if ( (!groupA.isEmpty && !groupB.isEmpty) && (groupA != groupB) && isCausalRelationValid(groupA, groupB))
    } yield new CausalGroup(groupA,groupB)
  }

  def isCausalRelationValid(groupA: Set[String], groupB: Set[String]): Boolean = {
    val flags = for {
      grA <- groupA
      grB <- groupB
      if ( (!grA.isEmpty && !grB.isEmpty) && (grA != grB))
    } yield allEventsAreInCausalityRelation(groupA, groupB)

    return flags.filter(flag => flag==false).isEmpty
  }

  def allEventsAreInCausalityRelation(groupA: Set[String], groupB: Set[String]): Boolean = {
    val pairs = for {
      eventA <- groupA
      eventB <- groupB
    } yield new Pair(eventA, eventB)

    for {
      pair <- pairs
    } yield if (!causalBc.value.contains(pair)) { return false }

    true
  }

}
