package alphaAlgorithm

import misc.{CausalGroup, FullPairsInfoMap, Pair, PairInfo, PairNotation}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import petriNet.PetriNet
import petriNet.flow.Edge
import petriNet.state.Places
import steps._
import tools.TraceTools

//TODO check for a better implementation of encoders
object AlphaAlgorithm {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val traceTools: TraceTools = new TraceTools()
    val logPath = "src/main/resources/realData.csv"
    val numOfTraces = 3

    val spark = SparkSession
      .builder()
      .appName("AlphaAlgorithm")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    val tracesDS : Dataset[(String, List[String])] = traceTools.readSpecificNumberOfTracesFromCsvFile(logPath, numOfTraces)

    val petriNet: PetriNet = executeAlphaAlgorithm(tracesDS)
    println(petriNet)

    // Stop the session
    spark.stop()
  }

  /**
    * Alpha algorithm execution consists of 8 steps.
    * The result is a PetriNet flow.
    * @param logPath
    * @return
    */
  def executeAlphaAlgorithm(tracesDS : Dataset[(String, List[String])]) : PetriNet = {

    val steps : AlphaAlgorithmSteps = new AlphaAlgorithmSteps()

    //Step 1 - Find all transitions / events, Sorted list of all event types
    val events = steps.getAllEvents(tracesDS)

    //Step 2 - Construct a set with all start activities (Ti)
    val startActivities = steps.getStartActivities(tracesDS)

    //Step 3 - Construct a set with all final activities (To)
    val finalActivities = steps.getFinalActivities(tracesDS)

    //Step 4 - Footprint graph - Causal groups
    val logRelations : Dataset[(Pair, String)] = steps.getFootprintGraph(tracesDS, events)
    val causalGroups : List[CausalGroup[String]] = steps.getCausalGroups(logRelations)

    //Step 5 - compute only maximal groups
    val maximalGroups : List[CausalGroup[String]] = steps.getMaximalGroups(causalGroups)

    //step 6 - set of places/states
    val places : Places = steps.getPlaces(maximalGroups, startActivities, finalActivities)

    //step 7 - set of arcs (flow)
    val edges : List[Edge] = steps.getEdges(places)

    //step 8 - construct petri net
    return new PetriNet(places, events, edges)
  }

}
