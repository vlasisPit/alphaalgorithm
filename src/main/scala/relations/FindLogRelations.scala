package relations

import misc.{Directionality, PairNotation, Relation}

/**
  * Find log relations
  * a->b iff a>b && !(b>a)
  * a||b iff a>b && b>a
  * a#b iff a>b && !(b>a)
  */
@SerialVersionUID(100L)
class FindLogRelations() extends Serializable {

  def findFootPrintGraph(pairInfo: (String, Set[PairNotation])): (String, String) = {
    val directFollow = pairInfo._2.toSeq
      .filter(x=> x.getDirectionality()==Directionality.DIRECT && x.getRelation()==Relation.FOLLOW)
      .toList

    val inverseFollow = pairInfo._2.toSeq
      .filter(x=> x.getDirectionality()==Directionality.INVERSE && x.getRelation()==Relation.FOLLOW)
      .toList

    if (directFollow.nonEmpty && inverseFollow.nonEmpty) {
      (pairInfo._1, Relation.PARALLELISM.toString)
    } else if (directFollow.nonEmpty && inverseFollow.isEmpty) {
      (pairInfo._1, Relation.CAUSALITY.toString)
    } else if (directFollow.isEmpty && inverseFollow.nonEmpty) {
      (pairInfo._1.reverse, Relation.CAUSALITY.toString)
    } else if (directFollow.isEmpty && inverseFollow.isEmpty) {
      (pairInfo._1, Relation.NEVER_FOLLOW.toString)
    } else {
      //this never must happen. Default case
      (pairInfo._1, Relation.FOLLOW.toString)
    }
  }

}
