package misc

@SerialVersionUID(100L)
class PairInfo(var pairInfo:(Pair, PairNotation)) extends Serializable {
  def getPairName(): Pair = {
    return pairInfo._1
  }

  def getPairNotation(): PairNotation = {
    return pairInfo._2
  }

  override def toString = s"PairInfo($getPairName, $getPairNotation)"
}
