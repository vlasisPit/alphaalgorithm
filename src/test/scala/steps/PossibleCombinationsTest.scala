package steps

import org.scalatest.FunSuite

class PossibleCombinationsTest extends FunSuite {

  test("Extract possible combinations from 3 member list") {
    val groupEvents : List[String] = List("B", "C", "E")

    val possibleCombinations : PossibleCombinations[String] = new PossibleCombinations(groupEvents);
    val combinations : List[Set[String]] = possibleCombinations.extractAllPossibleCombinations();

    assert(combinations.contains(Set()))
    assert(combinations.contains(Set("E")))
    assert(combinations.contains(Set("C")))
    assert(combinations.contains(Set("C","E")))
    assert(combinations.contains(Set("B")))
    assert(combinations.contains(Set("B","E")))
    assert(combinations.contains(Set("B","C")))
    assert(combinations.contains(Set("B","C","E")))

    assert(combinations.length==8)
  }

  test("Extract possible combinations from 4 member list") {
    val groupEvents : List[String] = List("B", "C", "D", "E")

    val possibleCombinations : PossibleCombinations[String] = new PossibleCombinations(groupEvents);
    val combinations : List[Set[String]] = possibleCombinations.extractAllPossibleCombinations();

    assert(combinations.contains(Set()))
    assert(combinations.contains(Set("E")))
    assert(combinations.contains(Set("D")))
    assert(combinations.contains(Set("D","E")))

    assert(combinations.contains(Set("C")))
    assert(combinations.contains(Set("C","E")))
    assert(combinations.contains(Set("C","D")))
    assert(combinations.contains(Set("C","D","E")))

    assert(combinations.contains(Set("B")))
    assert(combinations.contains(Set("B","E")))
    assert(combinations.contains(Set("B","D")))
    assert(combinations.contains(Set("B","D","E")))

    assert(combinations.contains(Set("B","C")))
    assert(combinations.contains(Set("B","C","E")))
    assert(combinations.contains(Set("B","C","D")))
    assert(combinations.contains(Set("B","C","D","E")))

    assert(combinations.length==16)
  }

}
