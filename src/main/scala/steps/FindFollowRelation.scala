package steps

import misc.{Directionality, FullPairsInfoMap, Pair, PairNotation, Relation}

@SerialVersionUID(100L)
class FindFollowRelation() extends Serializable {

  /**
    * traceWithCaseId (caseID, actual trace) example: (case1, ABCD)
    * Find follow (and not follow) relation in the form of (PairAB, (DIRECT, FOLLOW/NOT_FOLLOW), (INVERSE, FOLLOW/NOT_FOLLOW))
    * where for each pair we check if the DIRECT pair AB has a relation FOLLOW or NOT_FOLLOW
    * and the inverse pair direction (BA) has a relation FOLLOW or NOT_FOLLOW
    *
    * @param traceWithCaseId
    * @return
    */
  def findFollowRelation(traceWithCaseId: (String, List[String]), pairsToExamine: List[Pair]):FullPairsInfoMap = {
    var pairInfoMap = pairInfoInit(pairsToExamine)

    Range(0, traceWithCaseId._2.length-1)
      .map(i=> new Pair(traceWithCaseId._2(i), traceWithCaseId._2(i+1)))
      .map(pair=> {
          val pairExists : Boolean = checkIfPairExists(pair, pairInfoMap)
          pairInfoMap = matchPairExists(pairExists, pair, pairInfoMap)
        }
      )

    return new FullPairsInfoMap(pairInfoMap)
  }

  def matchPairExists(bool: Boolean, pair: Pair, pairInfoMap : Map[Pair, (PairNotation, PairNotation)]): Map[Pair, (PairNotation, PairNotation)] = bool match {
    case false => {//reverse relation in this case
        val tuple2 = pairInfoMap(new Pair(pair.getSecondMember(), pair.getFirstMember()))
        val newTuple2 = (tuple2._1,new PairNotation(Directionality.INVERSE, Relation.FOLLOW))
        pairInfoMap + (new Pair(pair.getSecondMember(), pair.getFirstMember()) -> newTuple2)
      }
    case true => {
        val tuple2 = pairInfoMap(pair)
        val newTuple2 = (new PairNotation((Directionality.DIRECT, Relation.FOLLOW)), tuple2._2)
        pairInfoMap + (pair -> newTuple2)
      }
  }

  def checkIfPairExists(pair: Pair, pairInfoMap: Map[Pair, (PairNotation, PairNotation)]) : Boolean = {
    return pairInfoMap.keys.toList.contains(pair)
  }

  /**
    * Initialize all potential pairs to examine as NOT_FOLLOW relations
    * @param pairsToExamine
    * @return
    */
  private def pairInfoInit(pairsToExamine: List[Pair]) : Map[Pair, (PairNotation, PairNotation)] = {
    pairsToExamine
      .map(pair => pair -> (new PairNotation((Directionality.DIRECT, Relation.NOT_FOLLOW)), new PairNotation((Directionality.INVERSE, Relation.NOT_FOLLOW))))
      .toMap
  }

}
