package relations

import misc.{Directionality, PairNotation, FullPairsInfoMap, Relation}

import scala.collection.mutable.ListBuffer

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
  def findFollowRelation(traceWithCaseId: (String, List[String]), pairsToExamine: List[String]):FullPairsInfoMap = {
    var pairInfoMap = pairInfoInit(pairsToExamine)

    for( i <- 0 to traceWithCaseId._2.length-2){
      var pair : String = traceWithCaseId._2(i)+traceWithCaseId._2(i+1)
      val pairExists : Boolean = checkIfPairExists(pair, pairInfoMap)
      if (pairExists == false) {
        //reverse relation in this case
        pair = pair.reverse
        val tuple2 = pairInfoMap(pair)
        val newTuple2 = (tuple2._1,new PairNotation(Directionality.INVERSE, Relation.FOLLOW))
        pairInfoMap = pairInfoMap + (pair -> newTuple2)
      } else {
        val tuple2 = pairInfoMap(pair)
        val newTuple2 = (new PairNotation((Directionality.DIRECT, Relation.FOLLOW)), tuple2._2)
        pairInfoMap = pairInfoMap + (pair -> newTuple2)
      }
    }

    return new FullPairsInfoMap(pairInfoMap)
  }

  def checkIfPairExists(pair: String, pairInfoMap: Map[String, (PairNotation, PairNotation)]) : Boolean = {
    return pairInfoMap.keys.toList.contains(pair)
  }

  /**
    * Initialize all potential pairs to examine as NOT_FOLLOW relations
    * @param pairsToExamine
    * @return
    */
  private def pairInfoInit(pairsToExamine: List[String]) : Map[String, (PairNotation, PairNotation)] = {
    var pairInfo: Map[String, (PairNotation, PairNotation)] = Map()

    for(i <- 0 to pairsToExamine.length-1) {
      pairInfo = pairInfo + (pairsToExamine(i) -> (new PairNotation((Directionality.DIRECT, Relation.NOT_FOLLOW)), new PairNotation((Directionality.INVERSE, Relation.NOT_FOLLOW))))
    }
    return pairInfo
  }

}
