package steps

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Find all possible sub-groups for a given data set
  * @param events
  */
class PossibleCombinations(val events: List[String]) {

  def extractAllPossibleCombinations(): List[Set[String]] = {
    (1 to events.size).flatMap(events.combinations)
      .map(x=>x.toSet)
      .toList
  }

}
