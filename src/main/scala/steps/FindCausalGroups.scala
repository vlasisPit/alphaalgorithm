package steps

import java.util.concurrent.TimeUnit

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
  * First of all, algorithm finds all causal relations by filtering these data
  * Secondly, algorithm computes causal groups from the first members to the second trying to make groups of the same
  * members.
  * Same, for the other side
  * @tparam T
  */
@SerialVersionUID(100L)
class FindCausalGroups(val logRelations: Dataset[(Pair, String)]) extends Serializable {
  implicit def pairEncoder: org.apache.spark.sql.Encoder[Pair] = org.apache.spark.sql.Encoders.kryo[Pair]
  implicit def causalGroupGenericEncoder: org.apache.spark.sql.Encoder[CausalGroup[String]] = org.apache.spark.sql.Encoders.kryo[CausalGroup[String]]
  implicit def setEncoder: org.apache.spark.sql.Encoder[Set[String]] = org.apache.spark.sql.Encoders.kryo[Set[String]]
  //implicit def tupleEncoder: org.apache.spark.sql.Encoder[String] = org.apache.spark.sql.Encoders.kryo[String]
  implicit def pairStringEncoder: org.apache.spark.sql.Encoder[(Pair, String)] = org.apache.spark.sql.Encoders.kryo[(Pair, String)]
  implicit def listCausalGroupsEncoder: org.apache.spark.sql.Encoder[List[CausalGroup[String]]] = org.apache.spark.sql.Encoders.kryo[List[CausalGroup[String]]]
  implicit def tuple2[A1, A2](
                               implicit e1: Encoder[A1],
                               e2: Encoder[A2]
                             ): Encoder[(A1,A2)] = Encoders.tuple[A1,A2](e1, e2)

  val spark = SparkSession.builder().getOrCreate()

  val t0 = System.nanoTime()
  val never = logRelations
    .filter(x=>x._2==Relation.NEVER_FOLLOW.toString)
    .map(neverRelation => neverRelation._1)
    .distinct()
    .collect()
    .toList

  val neverBc = spark.sparkContext.broadcast(never)
  println("Collect to list " + TimeUnit.SECONDS.convert(System.nanoTime() - t0, TimeUnit.NANOSECONDS) + "sec")

  def extractCausalGroups():Dataset[CausalGroup[String]] = {
    val directCausalGroups = logRelations
      .filter(x=>x._2==Relation.CAUSALITY.toString)
      .map(x=>x._1)
      .map(x=>new CausalGroup(Set(x.member1), Set(x.member2)))
      .distinct()

    val causalGroupsFromLeftPart = directCausalGroups
      .groupByKey(x=>x.getFirstGroup())
      .mapGroups{case(k, iter) => (k, iter.map(x => x.getSecondGroup().head).toSet)}
      .filter(x=>x._2.size>1)
      .map(x=>new CausalGroup(x._1, x._2))
      .distinct()
      .collect()
      .toList

    val causalGroupsFromLeft = causalGroupsFromLeftPart
      .flatMap(x=> createFinalCausalGroupsLeft(x))

    val causalGroupsFromRightPart = directCausalGroups
      .map(x=>new CausalGroup(x.getSecondGroup(), x.getFirstGroup()))
      .groupByKey(x=>x.getFirstGroup())
      .mapGroups{case(k, iter) => (k, iter.map(x => x.getSecondGroup().head).toSet)}
      .filter(x=>x._2.size>1)
      .map(x=>new CausalGroup(x._1, x._2))
      .distinct()
      .collect()
      .toList

    val causalGroupsFromRight = causalGroupsFromRightPart
      .flatMap(x=> createFinalCausalGroupsRight(x))

    import spark.implicits._
    return directCausalGroups
      .union(causalGroupsFromLeft.toDS())
      .union(causalGroupsFromRight.toDS());
  }

  /**
    * Input a causal group which we have to inspect for breaking to more groups,
    * if NeverFollow relation is not valid
    * Example causal group {a} -> {b,c,e}
    * to {a} -> {b,e} and {a} -> {c,e}
    */
  def createFinalCausalGroupsLeft(causalGroup: CausalGroup[String]): List[CausalGroup[String]] = {
    return checkIfNeverFollowRelationIsValidAndBreakTheGroup(causalGroup.getSecondGroup())
      .map(x => new CausalGroup(causalGroup.getFirstGroup(), x))
  }

  /**
    * Input a causal group which we have to inspect for breaking to more groups,
    * if NeverFollow relation is not valid
    * Example causal group {b,c,e} ->  {a}
    * to {b,e} -> {d} and {c,e} -> {d}
    */
  def createFinalCausalGroupsRight(causalGroup: CausalGroup[String]): List[CausalGroup[String]] = {
    return checkIfNeverFollowRelationIsValidAndBreakTheGroup(causalGroup.getSecondGroup())
      .map(x => new CausalGroup(x, causalGroup.getFirstGroup()))
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
    */
  def checkIfNeverFollowRelationIsValidAndBreakTheGroup(events: Set[String]): List[Set[String]] = {
    //eg {b,c,e}. Maybe these events are connected with some not NOT-FOLLOW relation, so the group must be broken
    val possibleCombinations : PossibleCombinations = new PossibleCombinations(events.toList)
    val allPossibleCombinations = possibleCombinations.extractAllPossibleCombinations()
    val groups = allPossibleCombinations
      .filter(x=>allRelationsAreNeverFollow(x))
      .filter(x=>x.size>1)

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
    val notNeverFollow = allPossiblePairs
      .find(x=> (!neverBc.value.contains(x) && !neverBc.value.contains(createInversePair(x))))

    notNeverFollow.isEmpty
  }

  def createInversePair(pair : Pair): Pair = {
    return new Pair(pair.getSecondMember(), pair.getFirstMember())
  }

/*  def isNeverFollow(pair: Pair): Boolean = {
    println("check ...")

    val t0 = System.nanoTime()
    val ckeckedPair = neverBc.value
      .filter(neverPair=>(neverPair.getFirstMember()==pair.getFirstMember() && neverPair.getSecondMember()==pair.getSecondMember())
        || (neverPair.getFirstMember()==pair.getSecondMember() &&neverPair.getSecondMember()==pair.getFirstMember()))
      .limit(1)

    val t1 = System.nanoTime()
    println("Elapsed time compute never follow each: All possible comb " + TimeUnit.SECONDS.convert(t1 - t0, TimeUnit.NANOSECONDS) + "sec")

    ckeckedPair.count() != 0;
  }*/

}
