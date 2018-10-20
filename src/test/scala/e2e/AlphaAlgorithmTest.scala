package e2e

import alphaAlgorithm.AlphaAlgorithm
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite
import petriNet.flow.Edge
import petriNet.state.State
import tools.TraceTools

/**
  * End-to-End tests to check the correct functionality of algorithm's implementation.
  */
class AlphaAlgorithmTest extends FunSuite {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession
    .builder()
    .appName("AlphaAlgorithm")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    .getOrCreate()

  test("Check Alpha Algorithm functionality - Log 1") {
    val logPath = "src/test/resources/log1.txt"
    val traceTools: TraceTools = new TraceTools()
    val tracesDS : Dataset[(String, List[String])] = traceTools.tracesDSFromLogFile(logPath)
    val petriNet = AlphaAlgorithm.executeAlphaAlgorithm(tracesDS)

    //check edges
    assert(petriNet.getEdges().contains(new Edge("A", new State(Set("A"), Set("B", "E")), true)))
    assert(petriNet.getEdges().contains(new Edge("B", new State(Set("A"), Set("B", "E")), false)))
    assert(petriNet.getEdges().contains(new Edge("E", new State(Set("A"), Set("B", "E")), false)))
    assert(petriNet.getEdges().contains(new Edge("A", new State(Set("A"), Set("C", "E")), true)))
    assert(petriNet.getEdges().contains(new Edge("C", new State(Set("A"), Set("C", "E")), false)))
    assert(petriNet.getEdges().contains(new Edge("E", new State(Set("A"), Set("C", "E")), false)))
    assert(petriNet.getEdges().contains(new Edge("B", new State(Set("B", "E"), Set("D")), true)))
    assert(petriNet.getEdges().contains(new Edge("E", new State(Set("B", "E"), Set("D")), true)))
    assert(petriNet.getEdges().contains(new Edge("D", new State(Set("B", "E"), Set("D")), false)))
    assert(petriNet.getEdges().contains(new Edge("C", new State(Set("C", "E"), Set("D")), true)))
    assert(petriNet.getEdges().contains(new Edge("E", new State(Set("C", "E"), Set("D")), true)))
    assert(petriNet.getEdges().contains(new Edge("D", new State(Set("C", "E"), Set("D")), false)))

    //check initial edges
    assert(petriNet.getEdges().contains(new Edge("A", new State(Set.empty, Set("A")), false)))

    //check final edges
    assert(petriNet.getEdges().contains(new Edge("D", new State(Set("D"), Set.empty), true)))

    //check wrong directionality
    assert(!petriNet.getEdges().contains(new Edge("B", new State(Set("B", "C"), Set("D")), false)))
    assert(!petriNet.getEdges().contains(new Edge("D", new State(Set("B", "C"), Set("D")), true)))

    assert(petriNet.getEdges().size==14)
  }

  test("Check Alpha Algorithm functionality - Log 2") {
    val logPath = "src/test/resources/log2.txt"
    val traceTools: TraceTools = new TraceTools()
    val tracesDS : Dataset[(String, List[String])] = traceTools.tracesDSFromLogFile(logPath)
    val petriNet = AlphaAlgorithm.executeAlphaAlgorithm(tracesDS)

    //check edges
    assert(petriNet.getEdges().contains(new Edge("A", new State(Set("A"), Set("B", "C")), true)))
    assert(petriNet.getEdges().contains(new Edge("B", new State(Set("A"), Set("B", "C")), false)))
    assert(petriNet.getEdges().contains(new Edge("C", new State(Set("A"), Set("B", "C")), false)))
    assert(petriNet.getEdges().contains(new Edge("B", new State(Set("B", "C"), Set("D")), true)))
    assert(petriNet.getEdges().contains(new Edge("C", new State(Set("B", "C"), Set("D")), true)))
    assert(petriNet.getEdges().contains(new Edge("D", new State(Set("B", "C"), Set("D")), false)))

    //check initial edges
    assert(petriNet.getEdges().contains(new Edge("A", new State(Set.empty, Set("A")), false)))

    //check final edges
    assert(petriNet.getEdges().contains(new Edge("D", new State(Set("D"), Set.empty), true)))

    //check wrong directionality
    assert(!petriNet.getEdges().contains(new Edge("B", new State(Set("B", "C"), Set("D")), false)))
    assert(!petriNet.getEdges().contains(new Edge("D", new State(Set("B", "C"), Set("D")), true)))

    assert(petriNet.getEdges().size==8)
  }

  test("Check Alpha Algorithm functionality - Log 3") {
    val logPath = "src/test/resources/log3.txt"
    val traceTools: TraceTools = new TraceTools()
    val tracesDS : Dataset[(String, List[String])] = traceTools.tracesDSFromLogFile(logPath)
    val petriNet = AlphaAlgorithm.executeAlphaAlgorithm(tracesDS)

    //check edges
    assert(petriNet.getEdges().contains(new Edge("A", new State(Set("A"), Set("B", "C")), true)))
    assert(petriNet.getEdges().contains(new Edge("B", new State(Set("A"), Set("B", "C")), false)))
    assert(petriNet.getEdges().contains(new Edge("C", new State(Set("A"), Set("B", "C")), false)))
    assert(petriNet.getEdges().contains(new Edge("C", new State(Set("C"), Set("G")), true)))
    assert(petriNet.getEdges().contains(new Edge("G", new State(Set("C"), Set("G")), false)))
    assert(petriNet.getEdges().contains(new Edge("B", new State(Set("B"), Set("D")), true)))
    assert(petriNet.getEdges().contains(new Edge("D", new State(Set("B"), Set("D")), false)))
    assert(petriNet.getEdges().contains(new Edge("D", new State(Set("D"), Set("F")), true)))
    assert(petriNet.getEdges().contains(new Edge("F", new State(Set("D"), Set("F")), false)))
    assert(petriNet.getEdges().contains(new Edge("B", new State(Set("B"), Set("E")), true)))
    assert(petriNet.getEdges().contains(new Edge("E", new State(Set("B"), Set("E")), false)))
    assert(petriNet.getEdges().contains(new Edge("E", new State(Set("E"), Set("F")), true)))
    assert(petriNet.getEdges().contains(new Edge("F", new State(Set("E"), Set("F")), false)))
    assert(petriNet.getEdges().contains(new Edge("F", new State(Set("F", "G"), Set("H")), true)))
    assert(petriNet.getEdges().contains(new Edge("G", new State(Set("F", "G"), Set("H")), true)))
    assert(petriNet.getEdges().contains(new Edge("H", new State(Set("F", "G"), Set("H")), false)))

    //check initial edges
    assert(petriNet.getEdges().contains(new Edge("A", new State(Set.empty, Set("A")), false)))

    //check final edges
    assert(petriNet.getEdges().contains(new Edge("H", new State(Set("H"), Set.empty), true)))

    //check wrong directionality
    assert(!petriNet.getEdges().contains(new Edge("B", new State(Set("B", "C"), Set("D")), false)))
    assert(!petriNet.getEdges().contains(new Edge("D", new State(Set("B", "C"), Set("D")), true)))

    assert(petriNet.getEdges().size==18)
  }

  test("Check Alpha Algorithm functionality - Log 4") {
    val logPath = "src/test/resources/log4.txt"
    val traceTools: TraceTools = new TraceTools()
    val tracesDS : Dataset[(String, List[String])] = traceTools.tracesDSFromLogFile(logPath)
    val petriNet = AlphaAlgorithm.executeAlphaAlgorithm(tracesDS)

    //check edges
    assert(petriNet.getEdges().contains(new Edge("A", new State(Set("A", "E"), Set("B")), true)))
    assert(petriNet.getEdges().contains(new Edge("E", new State(Set("A", "E"), Set("B")), true)))
    assert(petriNet.getEdges().contains(new Edge("B", new State(Set("A", "E"), Set("B")), false)))
    assert(petriNet.getEdges().contains(new Edge("B", new State(Set("B"), Set("C")), true)))
    assert(petriNet.getEdges().contains(new Edge("C", new State(Set("B"), Set("C")), false)))
    assert(petriNet.getEdges().contains(new Edge("C", new State(Set("C"), Set("D", "E")), true)))
    assert(petriNet.getEdges().contains(new Edge("D", new State(Set("C"), Set("D", "E")), false)))
    assert(petriNet.getEdges().contains(new Edge("E", new State(Set("C"), Set("D", "E")), false)))

    //check initial edges
    assert(petriNet.getEdges().contains(new Edge("A", new State(Set.empty, Set("A")), false)))

    //check final edges
    assert(petriNet.getEdges().contains(new Edge("D", new State(Set("D"), Set.empty), true)))

    //check wrong directionality
    assert(!petriNet.getEdges().contains(new Edge("B", new State(Set("B", "C"), Set("D")), false)))
    assert(!petriNet.getEdges().contains(new Edge("D", new State(Set("B", "C"), Set("D")), true)))

    assert(petriNet.getEdges().size==10)
  }

  test("Check Alpha Algorithm functionality - Log 5") {
    val logPath = "src/test/resources/log5.txt"
    val traceTools: TraceTools = new TraceTools()
    val tracesDS : Dataset[(String, List[String])] = traceTools.tracesDSFromLogFile(logPath)
    val petriNet = AlphaAlgorithm.executeAlphaAlgorithm(tracesDS)

    //check edges
    assert(petriNet.getEdges().contains(new Edge("A", new State(Set("A"), Set("B","D")), true)))
    assert(petriNet.getEdges().contains(new Edge("B", new State(Set("A"), Set("B","D")), false)))
    assert(petriNet.getEdges().contains(new Edge("D", new State(Set("A"), Set("B","D")), false)))
    assert(petriNet.getEdges().contains(new Edge("B", new State(Set("B"), Set("C")), true)))
    assert(petriNet.getEdges().contains(new Edge("C", new State(Set("B"), Set("C")), false)))
    assert(petriNet.getEdges().contains(new Edge("C", new State(Set("C","D"), Set("E")), true)))
    assert(petriNet.getEdges().contains(new Edge("D", new State(Set("C","D"), Set("E")), true)))
    assert(petriNet.getEdges().contains(new Edge("E", new State(Set("C","D"), Set("E")), false)))
    assert(petriNet.getEdges().contains(new Edge("E", new State(Set("E"), Set("F")), true)))
    assert(petriNet.getEdges().contains(new Edge("F", new State(Set("E"), Set("F")), false)))
    assert(petriNet.getEdges().contains(new Edge("E", new State(Set("E"), Set("G")), true)))
    assert(petriNet.getEdges().contains(new Edge("G", new State(Set("E"), Set("G")), false)))
    assert(petriNet.getEdges().contains(new Edge("F", new State(Set("F"), Set("H")), true)))
    assert(petriNet.getEdges().contains(new Edge("H", new State(Set("F"), Set("H")), false)))
    assert(petriNet.getEdges().contains(new Edge("G", new State(Set("G"), Set("H")), true)))
    assert(petriNet.getEdges().contains(new Edge("H", new State(Set("G"), Set("H")), false)))

    //check initial edges
    assert(petriNet.getEdges().contains(new Edge("A", new State(Set.empty, Set("A")), false)))

    //check final edges
    assert(petriNet.getEdges().contains(new Edge("H", new State(Set("H"), Set.empty), true)))

    //check wrong directionality
    assert(!petriNet.getEdges().contains(new Edge("B", new State(Set("B", "C"), Set("D")), false)))
    assert(!petriNet.getEdges().contains(new Edge("D", new State(Set("B", "C"), Set("D")), true)))

    assert(petriNet.getEdges().size==18)
  }

}
