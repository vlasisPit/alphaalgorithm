package steps

import misc.{Directionality, FullPairsInfoMap, Pair, Relation}
import org.scalatest.FunSuite

class FindFollowRelationTest extends FunSuite {

  test("FindFollowRelation.findFollowRelation") {
    val followRelation: FindFollowRelation = new FindFollowRelation()

    val trace: (String, List[String]) = ("case1", List("A", "B", "A", "C", "D"))
    val pairsToExamine: List[Pair] = List(
      new Pair("A","A"),
      new Pair("A","B"),
      new Pair("A","C"),
      new Pair("A","D"),
      new Pair("B","B"),
      new Pair("B","C"),
      new Pair("B","D"),
      new Pair("A","C"),
      new Pair("A","D"),
      new Pair("C","D")
    )

    val pairs: FullPairsInfoMap = followRelation.findFollowRelation(trace, pairsToExamine)

    assert(pairs.getPairsMap()(new Pair("A","A"))._1.getDirectionality()==Directionality.DIRECT)
    assert(pairs.getPairsMap()(new Pair("A","A"))._2.getDirectionality()==Directionality.INVERSE)
    assert(pairs.getPairsMap()(new Pair("A","A"))._1.getRelation()==Relation.NOT_FOLLOW)
    assert(pairs.getPairsMap()(new Pair("A","A"))._2.getRelation()==Relation.NOT_FOLLOW)

    assert(pairs.getPairsMap()(new Pair("A","B"))._1.getDirectionality()==Directionality.DIRECT)
    assert(pairs.getPairsMap()(new Pair("A","B"))._2.getDirectionality()==Directionality.INVERSE)
    assert(pairs.getPairsMap()(new Pair("A","B"))._1.getRelation()==Relation.FOLLOW)
    assert(pairs.getPairsMap()(new Pair("A","B"))._2.getRelation()==Relation.FOLLOW)
  }

}
