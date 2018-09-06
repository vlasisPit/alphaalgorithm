package relations

import org.scalatest.FunSuite

class FollowTest extends FunSuite {
  test("Follow.findFollowRelation") {
    val followRelation: Follow = new Follow()

    val trace: (String, List[String]) = ("case1", List("A", "B", "C", "D"))

    val pairs: List[(String, Boolean, Boolean)] = followRelation.findFollowRelation(trace)

    assert(pairs(0)._1 === "AB")
    assert(pairs(0)._2 === true)
    assert(pairs(0)._3 === false)

    assert(pairs(1)._1 === "BC")
    assert(pairs(1)._2 === true)
    assert(pairs(1)._3 === false)

    assert(pairs(2)._1 === "CD")
    assert(pairs(2)._2 === true)
    assert(pairs(2)._3 === false)
  }
}
