package steps

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

/**
  * Find all possible sub-groups for a given data set
  *
  * @param events
  * @tparam T
  */
class PossibleCombinations(val events: Dataset[String]) {

  def extractAllPossibleCombinations(): List[Set[String]] = {
    var groups : List[Set[String]] = List()

    for( i <- 0 to scala.math.pow(2,events.count()).toInt-1 ){
      val counterBinary = convertToBinary(i, events.count().toInt)

      var group : Set[String] = Set()
      for (j <- 0 to (counterBinary.length-1)) {
        if (counterBinary.charAt(j) == '1') {
          group = group + getSpecificElementFromDataset(j)
        }
      }
      groups = group :: groups

    }

    return groups
  }

  def getSpecificElementFromDataset(index: Int) : String = {
    val eventsRdd : RDD[String]= events.rdd

    eventsRdd
      .zipWithIndex
      .filter(x=>x._2==index)
      .map(x=>x._1)
      .first()
  }

  def convertToBinary(i: Int, targetLength: Int) : String = {
    val num : String = i.toBinaryString

    if (num.length==targetLength) {
      return num
    } else {
      val numberOfZerosToAdd = targetLength-num.length
      var zeros = ""
      for (i <- 0 to numberOfZerosToAdd-1) {
        zeros = zeros + "0"
      }

      return zeros + num
    }
  }

}
