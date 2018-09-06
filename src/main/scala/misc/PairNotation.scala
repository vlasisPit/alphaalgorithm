package misc

import misc.Directionality.Directionality
import misc.Relation.Relation

@SerialVersionUID(100L)
class PairNotation(var pairNotation:(Directionality, Relation)) extends Serializable {

  def getDirectionality(): Directionality = {
    return pairNotation._1
  }

  def getRelation(): Relation = {
    return pairNotation._2
  }

  override def toString = s"PairNotation($getDirectionality, $getRelation)"
}
