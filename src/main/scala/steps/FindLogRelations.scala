package steps

import misc.{Directionality, Pair, PairNotation, Relation}

/**
  * Find log relations
  * a->b iff a>b && !(b>a)
  * a||b iff a>b && b>a
  * a#b iff a>b && !(b>a)
  */
@SerialVersionUID(100L)
class FindLogRelations() extends Serializable {

  def getDirectAndInverseFollowRelations(pairInfo: (Pair, Set[PairNotation])): ((Pair, Set[PairNotation]), List[PairNotation], List[PairNotation]) = {
    val directFollow = pairInfo._2.toSeq
      .filter(x=> x.getDirectionality()==Directionality.DIRECT && x.getRelation()==Relation.FOLLOW)
      .toList

    val inverseFollow = pairInfo._2.toSeq
      .filter(x=> x.getDirectionality()==Directionality.INVERSE && x.getRelation()==Relation.FOLLOW)
      .toList

    (pairInfo, directFollow, inverseFollow)
  }

  def extractFootPrintGraph(pairInfo: (Pair, Set[PairNotation]), directFollow: List[PairNotation], inverseFollow: List[PairNotation]): (Pair, String) = {
    if (directFollow.nonEmpty && inverseFollow.nonEmpty) {
      (pairInfo._1, Relation.PARALLELISM.toString)
    } else if (directFollow.nonEmpty && inverseFollow.isEmpty) {
      (pairInfo._1, Relation.CAUSALITY.toString)
    } else if (directFollow.isEmpty && inverseFollow.nonEmpty) {
      (new Pair(pairInfo._1.member2, pairInfo._1.member1), Relation.CAUSALITY.toString)
    } else if (directFollow.isEmpty && inverseFollow.isEmpty) {
      (pairInfo._1, Relation.NEVER_FOLLOW.toString)
    } else {
      //this never must happen. Default case
      (pairInfo._1, Relation.FOLLOW.toString)
    }
  }

}
