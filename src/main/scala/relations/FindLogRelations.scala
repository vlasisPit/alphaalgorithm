package relations

import misc.{Directionality, PairNotation, Relation}

@SerialVersionUID(100L)
class FindLogRelations() extends Serializable {

  def findRelations(pairInfo: (String, Set[PairNotation])): (String, String) = {
    val directFollow = pairInfo._2.toSeq
      .filter(x=>(x.getDirectionality()==Directionality.DIRECT && x.getRelation()==Relation.FOLLOW))
      .toList;

    val directNotFollow = pairInfo._2.toSeq
      .filter(x=>(x.getDirectionality()==Directionality.DIRECT && x.getRelation()==Relation.NOT_FOLLOW))
      .toList;

    val inverseFollow = pairInfo._2.toSeq
      .filter(x=>(x.getDirectionality()==Directionality.INVERSE && x.getRelation()==Relation.FOLLOW))
      .toList;

    val inverseNotFollow = pairInfo._2.toSeq
      .filter(x=>(x.getDirectionality()==Directionality.INVERSE && x.getRelation()==Relation.NOT_FOLLOW))
      .toList;

    if (directFollow.length!=0 && inverseFollow.length!=0) {
      return (pairInfo._1, Relation.PARALLELISM.toString);
    } else if (directFollow.length!=0 && inverseFollow.length==0) {
      return (pairInfo._1, Relation.CAUSALITY.toString);
    } else if (directFollow.length==0 && inverseFollow.length!=0) {
      return (pairInfo._1.reverse, Relation.CAUSALITY.toString);
    } else if (directFollow.length==0 && inverseFollow.length==0) {
      return (pairInfo._1, Relation.NEVER_FOLLOW.toString);
    } else {
      return (pairInfo._1, Relation.FOLLOW.toString);
    }
  }

}
