package tools

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ListBuffer

@SerialVersionUID(100L)
class TraceTools extends Serializable {

  def tracesDSFromLogFile(logPath: String) : Dataset[(String, List[String])] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    //traces like (case1, List(A,B,C,D))
    val traces = spark.sparkContext
      .textFile(logPath)
      .map(x=>parseLine(x))

    // Convert to a DataSet
    val tracesDS = traces.toDS()
    tracesDS.cache()
    return tracesDS
  }

  def parseLine(line: String) = {
    val fields = line.split(" ")
    val caseId = fields.head
    val trace = fields.tail.toList
    (caseId, trace)
  }

  /**
    * We assume that the events list contains no duplicates
    * If the events are A,B,C,D,E then pairs for computation are
    * AA, AB AC AD
    * BB BC BD
    * AC AD
    * CD
    * @param events
    * @return
    */
  def constructPairsForComputationFromEvents(events: List[String]): List[String] = {
    var tempTrace = events
    var pairs = new ListBuffer[String]()

    for( i <- 0 to events.size-1) {
      for( j <- 0 to tempTrace.length-1) {
        val tuple2 = events(i)+tempTrace(j)
        pairs = pairs += tuple2
      }
      tempTrace = tempTrace.tail
    }

    return pairs.toList
  }

  /**
    * Construct pairs for computation from a trace, which may contains duplicate events
    * @param trace
    * @return
    */
  def constructPairsForComputationFromTrace(trace: List[String]): List[String] = {
    val traceWithNoDuplicates = trace.toSet
    var tempTrace = traceWithNoDuplicates.toList
    var pairs = new ListBuffer[String]()

    for( i <- 0 to traceWithNoDuplicates.toList.size-1) {
      for( j <- 0 to tempTrace.length-1) {
        val tuple2 = traceWithNoDuplicates.toList(i)+tempTrace(j)
        pairs = pairs += tuple2
      }
      tempTrace = tempTrace.tail
    }

    return pairs.toList
  }

}
