package steps

import misc.CausalGroup
import org.apache.spark.sql._

/**
  * extract_2 is deprecated by extract, because it was necessary to reduce the memory amount in order to run it
  * in a real dataset.
  * With the extract solution it is not necessary to compute all possible pair combinations and save them on memory.
  * @param causalGroups
  */
@SerialVersionUID(100L)
class FindMaximalPairs(val causalGroups: List[CausalGroup[String]]) extends Serializable {

  implicit def causalGroupGenericEncoder: org.apache.spark.sql.Encoder[CausalGroup[String]] = org.apache.spark.sql.Encoders.kryo[CausalGroup[String]]
  implicit def tuple2[A1, A2](
                               implicit e1: Encoder[A1],
                               e2: Encoder[A2]
                             ): Encoder[(A1,A2)] = Encoders.tuple[A1,A2](e1, e2)


  /**
    * From all causal groups, keep only the maximal. Example from
    * ({a},{b}) and ({a}, {b,c}) keep only ({a}, {b,c})
    * @return
    */
  def extract(): List[CausalGroup[String]] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    return causalGroups.toDS()
      .filter(x=>toBeRetained(x))
      .collect()
      .toList;
  }

  def toBeRetained(toCheck: CausalGroup[String]): Boolean = {
    return causalGroups.filter(x=>x!=toCheck && isSubsetOf(toCheck, x)).isEmpty;
  }

  /**
    * If true, the group2 must be retained
    * @param group1
    * @param group2
    * @return
    */
  def isSubsetOf(group1 : (CausalGroup[String]), group2 : CausalGroup[String]) : Boolean = {
    return group1.getFirstGroup().subsetOf(group2.getFirstGroup()) && group1.getSecondGroup().subsetOf(group2.getSecondGroup())
  }

  /**
    * From all causal groups, keep only the maximal. Example from
    * ({a},{b}) and ({a}, {b,c}) keep only ({a}, {b,c})
    * @return
    */
  @Deprecated
  def extract_2(): List[CausalGroup[String]] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val causalGroupsToDelete =possibleCausalGroupsCombinations()
      .toDS()
      .filter(x=>isSubsetOf(x._1, x._2))
      .map(x=>x._1)
      .collect()
      .toSet

    return causalGroups.toDS()
      .filter(x=> !causalGroupsToDelete.contains(x))
      .collect()
      .toList
  }

  /**
    * Suppose we have the following causal groups
    * (A,B) , (C,D) , (A,BE) , (A,CE)
    * We must find all pair combinations of these groups in order to check maximal pairs
    * (A,B) - (C,D)
    * (A,B) - (A,BE)
    * (A,B) - (A,CE)
    * (C,D) - (A,BE)
    * (C,D) - (A,CE)
    *( A,BE) - (A,CE)
    * @param groups
    * @return
    */
  @Deprecated
  def possibleCausalGroupsCombinations(): List[(CausalGroup[String], CausalGroup[String])] = {
    return for {
      (x, idxX) <- causalGroups.zipWithIndex
      (y, idxY) <- causalGroups.zipWithIndex
      if (idxX < idxY || idxX > idxY)
    } yield (x,y)
  }

}
