package relations

import scala.collection.mutable.ListBuffer

@SerialVersionUID(100L)
class Follow() extends Serializable {

  /**
    * traceWithCaseId (caseID, actual trace) example: (case1, ABCD)
    * Find follow relation in the form of (PairAB, directAccess, InverseAccess)
    * where directAccess is true when B follows A (AB) in the trace
    * and InverseAccess is true when A follows B (BA) in the trace
    *
    * @param traceWithCaseId
    * @return
    */
  def findFollowRelation(traceWithCaseId: (String, List[String])): List[(String, Boolean, Boolean)] = {
    var pairInfo = new ListBuffer[(String, Boolean, Boolean)]()
    for( i <- 0 to traceWithCaseId._2.length-2){
      val tuple3 = (traceWithCaseId._2(i)+traceWithCaseId._2(i+1),true,false)
      pairInfo = pairInfo +=  tuple3
    }
    return pairInfo.toList
  }

}
