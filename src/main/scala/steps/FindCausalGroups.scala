package steps

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
  * Let Q,R be two sets of activities. Then (Q,R) is a causal group iff there is a causal relation -> from each element of Q
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

  val neverFollowPairs = logRelations.filter(x=>x._2==Relation.NEVER_FOLLOW.toString).map(x=>x._1).distinct()
  val causalityRelationPairs = logRelations.filter(x=>x._2==Relation.CAUSALITY.toString).map(x=>x._1).distinct()

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
    logRelations.cache()
    val directCausalRelations = logRelations
      .filter(x=>x._2==Relation.CAUSALITY.toString)
      .map(x=>x._1)
      .map(x=>(x.member1, x.member2))

    logRelations.unpersist()
    directCausalRelations.cache()

    val uniqueEventsFromLeftSideEvents = directCausalRelations
      .map(causal => causal._1)
      .distinct()
      .collect()
      .toList

    val causalGroupFromLeftSide = extractCausalGroupPart(uniqueEventsFromLeftSideEvents);

    val uniqueEventsFromRightSideEvents = directCausalRelations
      .map(causal => causal._2)
      .distinct()
      .collect()
      .toList

    val causalGroupFromRightSide = extractCausalGroupPart(uniqueEventsFromRightSideEvents);

    computeCausalGroups(causalGroupFromLeftSide, causalGroupFromRightSide)
  }

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
    } yield if (!isCausalityRelation(pair)) { return false }

    true
  }

  def extractCausalGroupPart(uniqueEvents: List[String]): List[Set[String]] = {
    val possibleCombinations : PossibleCombinations[String] = new PossibleCombinations(uniqueEvents);
    val combinations : List[Set[String]] = possibleCombinations.extractAllPossibleCombinations();
    combinations
      .filter(subCategory=>allRelationsAreNeverFollow(subCategory))
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
    val numberOfNeverFollowPairs = allPossiblePairs
      .filter(pair=> isNeverFollowed(pair) || isNeverFollowed(createInversePair(pair)))
      .toList.length

    if (numberOfNeverFollowPairs == allPossiblePairs.size) true else false
  }

  def createInversePair(pair : Pair): Pair = {
    return new Pair(pair.getSecondMember(), pair.getFirstMember())
  }

  def isNeverFollowed(pair: Pair): Boolean = {
    return neverFollowPairs.filter(x=>x==pair).count()!=0;
  }

  def isCausalityRelation(pair: Pair): Boolean = {
    return causalityRelationPairs.filter(x=>x==pair).count()!=0;
  }

}
