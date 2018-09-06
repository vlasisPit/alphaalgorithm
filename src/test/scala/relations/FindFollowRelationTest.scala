package relations

import misc.{Directionality, FullPairsInfoMap, Relation}
import org.scalatest.FunSuite

class FindFollowRelationTest extends FunSuite {
  test("FindFollowRelation.constructPairsForComputation") {
    val followRelation: FindFollowRelation = new FindFollowRelation()

    val trace: (String, List[String]) = ("case1", List("A", "B", "A", "C", "D"))

    val pairs: List[(String)] = followRelation.constructPairsForComputation(trace._2)

    assert(pairs.contains("AA"))
    assert(pairs.contains("AB"))
    assert(pairs.contains("AC"))
    assert(pairs.contains("AD"))
    assert(pairs.contains("BB"))
    assert(pairs.contains("BC"))
    assert(pairs.contains("BD"))
    assert(pairs.contains("AC"))
    assert(pairs.contains("AD"))
    assert(pairs.contains("CD"))

    assert(!pairs.contains("BA"))
    assert(!pairs.contains("CA"))
    assert(!pairs.contains("DA"))
    assert(!pairs.contains("CB"))
  }

  test("FindFollowRelation.findFollowRelation") {
    val followRelation: FindFollowRelation = new FindFollowRelation()

    val trace: (String, List[String]) = ("case1", List("A", "B", "A", "C", "D"))

    val pairs: FullPairsInfoMap = followRelation.findFollowRelation(trace)

    assert(pairs.getPairsMap()("AA")._1.getDirectionality()==Directionality.DIRECT)
    assert(pairs.getPairsMap()("AA")._2.getDirectionality()==Directionality.INVERSE)
    assert(pairs.getPairsMap()("AA")._1.getRelation()==Relation.NOT_FOLLOW)
    assert(pairs.getPairsMap()("AA")._2.getRelation()==Relation.NOT_FOLLOW)

    assert(pairs.getPairsMap()("AB")._1.getDirectionality()==Directionality.DIRECT)
    assert(pairs.getPairsMap()("AB")._2.getDirectionality()==Directionality.INVERSE)
    assert(pairs.getPairsMap()("AB")._1.getRelation()==Relation.FOLLOW)
    assert(pairs.getPairsMap()("AB")._2.getRelation()==Relation.FOLLOW)
  }
}
