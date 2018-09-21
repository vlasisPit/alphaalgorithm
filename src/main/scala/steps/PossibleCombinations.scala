package steps

/**
  * Find all possible sub-groups for a given data set
  * @param events
  * @tparam T
  */
class PossibleCombinations[T](val events: List[T]) {

  def extractAllPossibleCombinations(): List[Set[T]] = {
    var groups : List[Set[T]] = List()

    for( i <- 0 to scala.math.pow(2,events.size).toInt-1 ){
      val counterBinary = convertToBinary(i, events.size)

      var group : Set[T] = Set()
      for (j <- 0 to (counterBinary.length-1)) {
        if (counterBinary.charAt(j) == '1') {
          group = group + events(j)
        }
      }
      groups = group :: groups

    }

    return groups
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
