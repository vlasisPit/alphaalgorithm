package steps

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Find all possible sub-groups for a given data set
  * @param events
  */
@SerialVersionUID(100L)
class PossibleCombinations(val events: Dataset[String]) extends Serializable {
  val spark = SparkSession.builder().getOrCreate()
  implicit def stringEncoder: org.apache.spark.sql.Encoder[String] = org.apache.spark.sql.Encoders.kryo[String]
  implicit def stringTupleEncoder: org.apache.spark.sql.Encoder[(String, String)] = org.apache.spark.sql.Encoders.kryo[(String, String)]
  implicit def setStringEncoder: org.apache.spark.sql.Encoder[Set[String]] = org.apache.spark.sql.Encoders.kryo[Set[String]]
  implicit def intStringEncoder: org.apache.spark.sql.Encoder[(Int, String)] = org.apache.spark.sql.Encoders.kryo[(Int, String)]
  implicit def listStringEncoder: org.apache.spark.sql.Encoder[(String, List[String])] = org.apache.spark.sql.Encoders.kryo[(String, List[String])]

  def extractAllPossibleCombinations(): List[Set[String]] = {
    val eventsNum = events.count().toInt

    val eventsList = events
      .map(ev=>(eventsNum.toString, ev))
      .groupByKey(x=>x._1)
      .mapGroups{case(k, iter) => (k, iter.map(x => x._2).toList)}
      .limit(1)
      .head()
      ._2

    (1 to eventsNum).flatMap(eventsList.combinations)
        .map(x=>x.toSet)
        .toList
  }

}
