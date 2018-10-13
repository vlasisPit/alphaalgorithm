package tools

import org.scalatest.FunSuite

class TraceToolsTest extends FunSuite  {

  test("TraceTools.constructPairsForComputationFromEvents") {
    val traceTools: TraceTools = new TraceTools()

    val events: List[String] = List("A", "B", "C", "D", "E")

    val pairs: List[(String)] = traceTools.constructPairsForComputationFromEvents(events)

    assert(pairs.contains("AA"))
    assert(pairs.contains("AB"))
    assert(pairs.contains("AC"))
    assert(pairs.contains("AD"))
    assert(pairs.contains("AE"))
    assert(pairs.contains("BB"))
    assert(pairs.contains("BC"))
    assert(pairs.contains("BD"))
    assert(pairs.contains("BE"))
    assert(pairs.contains("CC"))
    assert(pairs.contains("CD"))
    assert(pairs.contains("CE"))
    assert(pairs.contains("DD"))
    assert(pairs.contains("DE"))
    assert(pairs.contains("EE"))
    assert(pairs.length==15)

    assert(!pairs.contains("BA"))
    assert(!pairs.contains("CA"))
    assert(!pairs.contains("DA"))
    assert(!pairs.contains("CB"))
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
