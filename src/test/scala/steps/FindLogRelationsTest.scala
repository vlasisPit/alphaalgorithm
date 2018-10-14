package steps

import misc.{PairNotation, Relation, Directionality, Pair}
import org.scalatest.FunSuite

class FindLogRelationsTest extends FunSuite {
  test("FindLogRelations.findRelations with PARALLELISM result") {
    val logRelations: FindLogRelations = new FindLogRelations()

    val pairInfo: (Pair, Set[PairNotation]) = (new Pair("A","B"), Set(
        new PairNotation((Directionality.DIRECT ,Relation.FOLLOW)),
        new PairNotation((Directionality.INVERSE ,Relation.FOLLOW)),
        new PairNotation((Directionality.DIRECT ,Relation.NOT_FOLLOW)),
        new PairNotation((Directionality.INVERSE ,Relation.NOT_FOLLOW))
    ))

    val directInverseFollowRel = logRelations.getDirectAndInverseFollowRelations(pairInfo)
    val relation:(Pair, String) = logRelations.extractFootPrintGraph(directInverseFollowRel._1, directInverseFollowRel._2, directInverseFollowRel._3)

    assert(relation._2.equals(Relation.PARALLELISM.toString))
  }

  test("FindLogRelations.findRelations with CAUSALITY result") {
    val logRelations: FindLogRelations = new FindLogRelations()

    val pairInfo: (Pair, Set[PairNotation]) = (new Pair("E","A"), Set(
      new PairNotation((Directionality.DIRECT ,Relation.FOLLOW)),
      new PairNotation((Directionality.INVERSE ,Relation.NOT_FOLLOW))
    ))

    val directInverseFollowRel = logRelations.getDirectAndInverseFollowRelations(pairInfo)
    val relation:(Pair, String) = logRelations.extractFootPrintGraph(directInverseFollowRel._1, directInverseFollowRel._2, directInverseFollowRel._3)

    assert(relation._2.equals(Relation.CAUSALITY.toString))
  }

  test("FindLogRelations.findRelations with CAUSALITY inverse result") {
    val logRelations: FindLogRelations = new FindLogRelations()

    val pairInfo: (Pair, Set[PairNotation]) = (new Pair("E","B"), Set(
      new PairNotation((Directionality.INVERSE ,Relation.FOLLOW)),
      new PairNotation((Directionality.DIRECT ,Relation.NOT_FOLLOW))
    ))

    val directInverseFollowRel = logRelations.getDirectAndInverseFollowRelations(pairInfo)
    val relation:(Pair, String) = logRelations.extractFootPrintGraph(directInverseFollowRel._1, directInverseFollowRel._2, directInverseFollowRel._3)

    assert(relation._1.getFirstMember().equals("B"))
    assert(relation._1.getSecondMember().equals("E"))
    assert(relation._2.equals(Relation.CAUSALITY.toString))
  }

  test("FindLogRelations.findRelations with NEVER_FOLLOW result") {
    val logRelations: FindLogRelations = new FindLogRelations()

    val pairInfo: (Pair, Set[PairNotation]) = (new Pair("A","D"), Set(
      new PairNotation((Directionality.DIRECT ,Relation.NOT_FOLLOW)),
      new PairNotation((Directionality.INVERSE ,Relation.NOT_FOLLOW))
    ))

    val directInverseFollowRel = logRelations.getDirectAndInverseFollowRelations(pairInfo)
    val relation:(Pair, String) = logRelations.extractFootPrintGraph(directInverseFollowRel._1, directInverseFollowRel._2, directInverseFollowRel._3)

    assert(relation._2.equals(Relation.NEVER_FOLLOW.toString))
  }
}
