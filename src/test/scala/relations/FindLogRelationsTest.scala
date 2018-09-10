package relations

import misc.{PairNotation, Relation, Directionality}
import org.scalatest.FunSuite

class FindLogRelationsTest extends FunSuite {
  test("FindLogRelations.findRelations with PARALLELISM result") {
    val logRelations: FindLogRelations = new FindLogRelations()

    val pairInfo: (String, Set[PairNotation]) = ("AB", Set(
        new PairNotation((Directionality.DIRECT ,Relation.FOLLOW)),
        new PairNotation((Directionality.INVERSE ,Relation.FOLLOW)),
        new PairNotation((Directionality.DIRECT ,Relation.NOT_FOLLOW)),
        new PairNotation((Directionality.INVERSE ,Relation.NOT_FOLLOW))
    ))

    val relation:(String, String) = logRelations.findFootPrintGraph(pairInfo)

    assert(relation._2.equals(Relation.PARALLELISM.toString))
  }

  test("FindLogRelations.findRelations with CAUSALITY result") {
    val logRelations: FindLogRelations = new FindLogRelations()

    val pairInfo: (String, Set[PairNotation]) = ("EA", Set(
      new PairNotation((Directionality.DIRECT ,Relation.FOLLOW)),
      new PairNotation((Directionality.INVERSE ,Relation.NOT_FOLLOW))
    ))

    val relation:(String, String) = logRelations.findFootPrintGraph(pairInfo)

    assert(relation._2.equals(Relation.CAUSALITY.toString))
  }

  test("FindLogRelations.findRelations with CAUSALITY inverse result") {
    val logRelations: FindLogRelations = new FindLogRelations()

    val pairInfo: (String, Set[PairNotation]) = ("EB", Set(
      new PairNotation((Directionality.INVERSE ,Relation.FOLLOW)),
      new PairNotation((Directionality.DIRECT ,Relation.NOT_FOLLOW))
    ))

    val relation:(String, String) = logRelations.findFootPrintGraph(pairInfo)

    assert(relation._1.equals("BE"))
    assert(relation._2.equals(Relation.CAUSALITY.toString))
  }

  test("FindLogRelations.findRelations with NEVER_FOLLOW result") {
    val logRelations: FindLogRelations = new FindLogRelations()

    val pairInfo: (String, Set[PairNotation]) = ("AD", Set(
      new PairNotation((Directionality.DIRECT ,Relation.NOT_FOLLOW)),
      new PairNotation((Directionality.INVERSE ,Relation.NOT_FOLLOW))
    ))

    val relation:(String, String) = logRelations.findFootPrintGraph(pairInfo)

    assert(relation._2.equals(Relation.NEVER_FOLLOW.toString))
  }
}
