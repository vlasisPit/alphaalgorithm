package relations

import misc.{CausalGroup, Pair, Relation}
import org.apache.spark.sql._

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
class FindCausalGroups(val logRelations: Dataset[(Pair, String)]) extends Serializable {

  val spark = SparkSession.builder().getOrCreate()
  val neverFollowPairs = logRelations.filter(x=>x._2==Relation.NEVER_FOLLOW.toString).map(x=>x._1).collect.toList

  implicit def pairEncoder: org.apache.spark.sql.Encoder[Pair] = org.apache.spark.sql.Encoders.kryo[Pair]
  implicit def causalGroupGenericEncoder: org.apache.spark.sql.Encoder[CausalGroup[String]] = org.apache.spark.sql.Encoders.kryo[CausalGroup[String]]
  implicit def setEncoder: org.apache.spark.sql.Encoder[Set[String]] = org.apache.spark.sql.Encoders.kryo[Set[String]]
  implicit def tupleEncoder: org.apache.spark.sql.Encoder[String] = org.apache.spark.sql.Encoders.kryo[String]
  implicit def rowEncoder: org.apache.spark.sql.Encoder[Row] = org.apache.spark.sql.Encoders.kryo[Row]
  implicit def pairStringEncoder: org.apache.spark.sql.Encoder[(Pair, String)] = org.apache.spark.sql.Encoders.kryo[(Pair, String)]
  implicit def listCausalGroupsEncoder: org.apache.spark.sql.Encoder[List[CausalGroup[String]]] = org.apache.spark.sql.Encoders.kryo[List[CausalGroup[String]]]
  implicit def tuple2[A1, A2](
                               implicit e1: Encoder[A1],
                               e2: Encoder[A2]
                             ): Encoder[(A1,A2)] = Encoders.tuple[A1,A2](e1, e2)

  def extractCausalGroups():List[CausalGroup[String]] = {
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
  def checkNeverFollowRelation(causalGroupNotCompleted: CausalGroup[String]): List[CausalGroup[String]] = {
    //eg {b,c,e}. Maybe these events are connected with some not NOT-FOLLOW relation, so the group must be broken
    val secondMembers = causalGroupNotCompleted.getSecondGroup().toList

    import spark.implicits._
    val possibleCombinations : PossibleCombinations[String] = new PossibleCombinations(secondMembers)
    val allPossibleCombinations = possibleCombinations.extractAllPossibleCombinations().toDS()
    val groups = allPossibleCombinations.filter(x=>allRelationsAreNeverFollow(x))
      .map(x => new CausalGroup[String](causalGroupNotCompleted.getFirstGroup(), x))
      .collect().toList

    return groups
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
    val numberOfNeverFollowPairs = allPossiblePairs.filter(x=> neverFollowPairs.contains(x)).toList.length

    if (numberOfNeverFollowPairs == allPossiblePairs.size) true else false
  }

}
