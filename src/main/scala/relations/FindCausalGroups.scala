package relations

import misc.{CausalGroup, Pair, Relation}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

/**
  * Let Q,R be set of activities. Then (Q,R) is a causal group iff there is a causal relation -> from each element of Q
  * to each element of R (ie all pairwise combinations of elements of Q and R are in ->) and the members of Q and R are
  * not in ||
  * @tparam T
  */
@SerialVersionUID(100L)
class FindCausalGroups[T](val logRelations: Dataset[(Pair[T], T)]) extends Serializable {

  implicit def pairEncoder: org.apache.spark.sql.Encoder[Pair[T]] = org.apache.spark.sql.Encoders.kryo[Pair[T]]
  implicit def causalGroupGenericEncoder: org.apache.spark.sql.Encoder[CausalGroup[T]] = org.apache.spark.sql.Encoders.kryo[CausalGroup[T]]
  implicit def setEncoder: org.apache.spark.sql.Encoder[Set[T]] = org.apache.spark.sql.Encoders.kryo[Set[T]]
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

    causalGroupsFromLeft.foreach(x=>println(x.toString))

    val causalGroupsFromRight = directCausalGroups
      .map(x=>new CausalGroup(x.getSecondGroup(), x.getFirstGroup()))
      .groupByKey(x=>x.getFirstGroup())
      .mapGroups{case(k, iter) => (k, iter.map(x => x.getSecondGroup().head).toSet)}
      .filter(x=>x._2.size>1)
      .map(x=>new CausalGroup(x._2, x._1))

    causalGroupsFromRight.foreach(x=>println(x.toString))

    return directCausalGroups.collect.toList
  }

}