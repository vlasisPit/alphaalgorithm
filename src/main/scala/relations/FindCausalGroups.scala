package relations

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
  * Let Q,R be set of activities. Then (Q,R) is a causal group iff there is a causal relation -> from each element of Q
  * to each element of R (ie all pairwise combinations of elements of Q and R are in ->) and the members of Q and R are
  * not in ||
  *
  * First of all, algorithm finds all causal relations by filtering these data
  * Secondly, algorithm computes causal groups from the first members to the second trying to make groups of the same
  * members.
  * Same, for the other side
  * @tparam T
  */
@SerialVersionUID(100L)
class FindCausalGroups[T](val logRelations: Dataset[(Pair[T], T)]) extends Serializable {

  val spark = SparkSession.builder().getOrCreate()
  implicit def pairEncoder: org.apache.spark.sql.Encoder[Pair[T]] = org.apache.spark.sql.Encoders.kryo[Pair[T]]
  implicit def causalGroupGenericEncoder: org.apache.spark.sql.Encoder[CausalGroup[T]] = org.apache.spark.sql.Encoders.kryo[CausalGroup[T]]
  implicit def setEncoder: org.apache.spark.sql.Encoder[Set[T]] = org.apache.spark.sql.Encoders.kryo[Set[T]]
  implicit def listCausalGroupsEncoder: org.apache.spark.sql.Encoder[List[CausalGroup[T]]] = org.apache.spark.sql.Encoders.kryo[List[CausalGroup[T]]]
  implicit def tuple2[A1, A2](
                               implicit e1: Encoder[A1],
                               e2: Encoder[A2]
                             ): Encoder[(A1,A2)] = Encoders.tuple[A1,A2](e1, e2)

  def extractCausalGroups():List[CausalGroup[T]] = {
    val directCausalGroups = logRelations
      .filter(x=>x._2==Relation.CAUSALITY.toString)
      .map(x=>x._1)
      .map(x=>new CausalGroup(Set(x.member1), Set(x.member2)))

    directCausalGroups.foreach(x=>println(x.toString))

    val causalGroupsFromLeft = directCausalGroups
      .groupByKey(x=>x.getFirstGroup())
      .mapGroups{case(k, iter) => (k, iter.map(x => x.getSecondGroup().head).toSet)}
      .filter(x=>x._2.size>1)
      .map(x=>new CausalGroup(x._1, x._2))
      //.map(x=>checkNotFollowRelation(x))

    causalGroupsFromLeft.foreach(x=>println(x.toString))

    val causalGroupsFromRight = directCausalGroups
      .map(x=>new CausalGroup(x.getSecondGroup(), x.getFirstGroup()))
      .groupByKey(x=>x.getFirstGroup())
      .mapGroups{case(k, iter) => (k, iter.map(x => x.getSecondGroup().head).toSet)}
      .filter(x=>x._2.size>1)
      .map(x=>new CausalGroup(x._2, x._1))

    causalGroupsFromRight.foreach(x=>println(x.toString))

    return directCausalGroups.collect.toList :::
            causalGroupsFromLeft.collect.toList :::
            causalGroupsFromRight.collect.toList
  }

  /**
    * From causal groups provided, we must check if the relation NEVER_FOLLOW is valid.
    * If not the causal group must be broken to more groups.
    * For example suppose we have a causal group
    * {a} -> {b,c,e}
    * The next step is to check if for all events in {b,c,e}, all events relations are NEVER_FOLLOW.
    * If yes, {b,c,e} stay as is.
    * If not the algorithm must break down the set in more sets for which NEVER_FOLLOW is valid.
    *
    * @param causalGroupNotCompleted
    * @return
    */
  def checkNeverFollowRelation(causalGroupNotCompleted: CausalGroup[T]): List[CausalGroup[T]] = {
    //eg {b,c,e}. Maybe these events are connected with some not NOT-FOLLOW relation, so the group must be broken
    val secondMembers = causalGroupNotCompleted.getSecondGroup().toList

    import spark.implicits._
    val possibleCombinations : PossibleCombinations[T] = new PossibleCombinations(secondMembers);
    val allPossibleCombinations = possibleCombinations.extractAllPossibleCombinations().toDS()
    allPossibleCombinations.filter(x=>notNeverFollowRelationExists(x))

    return null
  }

  /**
    * If there is at least one not-NeverFollow relation then the group must be removed
    * Example lets suppose possibleGroup= {b,c,e} and there is b||c, b#e and e#c.
    * Then the output must be {b,e} and {c,e}
    * @param possibleGroup
    * @return
    */
  def notNeverFollowRelationExists(possibleGroup: Set[T]): Boolean = {
    val allPossiblePairs = for(x <- possibleGroup; y <- possibleGroup) yield (x, y)
    val logRelationsDataframe = logRelations.toDF("pair","relation")
    logRelationsDataframe.createOrReplaceTempView("relations")

    import spark.sql
    val neverFollowPair = allPossiblePairs.map(pair=>sql("SELECT relation FROM relations WHERE pair =" + pair))
      .map(relation => relation.col("relation"))
      .map(relation => relation.getItem())
      .find(relation=> !relation==Relation.NEVER_FOLLOW.toString) //other than Relation.NEVER_FOLLOW

    if (neverFollowPair.isEmpty) true else false
  }

}
