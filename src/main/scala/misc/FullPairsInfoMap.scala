package misc

/**
  * Contains full info about a pair, both for direct and inverse direction
  * @param map
  */
@SerialVersionUID(100L)
class FullPairsInfoMap(var map: Map[Pair, (PairNotation, PairNotation)]) extends Serializable {

  def getPairsMap(): Map[Pair, (PairNotation, PairNotation)] = {
    return map
  }
}
