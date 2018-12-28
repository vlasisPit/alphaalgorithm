package steps

import misc.CausalGroup
import org.apache.spark.sql._
import org.apache.spark.util.CollectionAccumulator

import scala.collection.JavaConverters._

/**
  * extract_2 is deprecated by extract, because it was necessary to reduce the memory amount in order to run it
  * in a real dataset.
  * With the extract solution it is not necessary to compute all possible pair combinations and save them on memory.
  * @param causalGroups
  */
@SerialVersionUID(100L)
class FindMaximalPairs(val causalGroups: Dataset[CausalGroup[String]]) extends Serializable {

  val spark = SparkSession.builder().getOrCreate()
  val causalGroupsList = causalGroups
    .distinct()
    .collect()
    .toList

  val causalGroupsListBc = spark.sparkContext.broadcast(causalGroupsList)

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
    val maximalCausalGroups : CollectionAccumulator[CausalGroup[String]] = spark.sparkContext.collectionAccumulator("maximalCausalGroups")

    causalGroups
      .foreach(group => if (toBeRetained(group)) {
        maximalCausalGroups.add(group)
      })

    maximalCausalGroups.value.asScala.toList
  }

  def toBeRetained(toCheck: CausalGroup[String]): Boolean = {
    !causalGroupsListBc.value.exists(x => x != toCheck && isSubsetOf(toCheck, x))
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

}
