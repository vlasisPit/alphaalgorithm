package tools

import org.scalatest.FunSuite
import misc.Pair

class TraceToolsTest extends FunSuite  {

  test("TraceTools.constructPairsForComputationFromEvents") {
    val traceTools: TraceTools = new TraceTools()

    val events: List[String] = List("A", "B", "C", "D", "E")

    val pairs: List[Pair] = traceTools.constructPairsForComputationFromEvents(events)

    assert(pairs.contains(new Pair("A", "A")))
    assert(pairs.contains(new Pair("A", "B")))
    assert(pairs.contains(new Pair("A", "C")))
    assert(pairs.contains(new Pair("A", "D")))
    assert(pairs.contains(new Pair("A", "E")))
    assert(pairs.contains(new Pair("B", "B")))
    assert(pairs.contains(new Pair("B", "C")))
    assert(pairs.contains(new Pair("B", "D")))
    assert(pairs.contains(new Pair("B", "E")))
    assert(pairs.contains(new Pair("C", "C")))
    assert(pairs.contains(new Pair("C", "D")))
    assert(pairs.contains(new Pair("C", "E")))
    assert(pairs.contains(new Pair("D", "D")))
    assert(pairs.contains(new Pair("D", "E")))
    assert(pairs.contains(new Pair("E", "E")))
    assert(pairs.length==15)

    assert(!pairs.contains(new Pair("B", "A")))
    assert(!pairs.contains(new Pair("C", "A")))
    assert(!pairs.contains(new Pair("D", "A")))
    assert(!pairs.contains(new Pair("C", "B")))
  }

  test("TraceTools.constructPairsForComputationFromTrace") {
    val traceTools: TraceTools = new TraceTools()

    val trace: (String, List[String]) = ("case1", List("A", "B", "A", "C", "D"))

    val pairs: List[(String)] = traceTools.constructPairsForComputationFromTrace(trace._2)

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

}
