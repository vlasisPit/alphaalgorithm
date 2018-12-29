package tools

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

import scala.collection.mutable.ListBuffer
import misc.{CausalGroup, Pair}

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
    return tracesDS
  }

  def parseLine(line: String) = {
    val fields = line.split(" ")
    val caseId = fields.head
    val trace = fields.tail.toList
    (caseId, trace)
  }

  /**
    * Read a specific number of traces (numOfTraces: Int) from a CSV file provided in path.
    * The CSV must contains the following columns
    * orderid", "eventname", "starttime", "endtime", "status"
    * Only logs with status==Completed are examined
    * @param path
    * @param numOfTraces
    * @return
    */
  def readSpecificNumberOfTracesFromCsvFile(path: String, numOfTraces: Int) : Dataset[(String, List[String])] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv").option("header", "true").load(path)

    val orderIds = df.select("orderid")
      .distinct()
      .limit(numOfTraces)
      .as(Encoders.STRING)
      .collect()
      .toList

    //Dataset[(String, List[String])]
    return df.select("orderid", "eventname", "starttime", "endtime", "status")
      .where( df("orderid").isin(orderIds:_*))
      .filter(df("status").isin(List("Completed"):_*))  //filtering - only the Completed traces
      .orderBy("starttime")
      .map(x=>(x.get(0).toString,x.get(1).toString))
      .groupByKey(x=>x._1)
      .mapGroups{case(k, iter) => (k, iter.map(x => x._2).toList)}  //toList in order to keep the order of the events
  }

  /**
    * Read all traces from a CSV file
    * @param path
    * @return
    */
  def readAllTracesFromCsvFile(path: String) : Dataset[(String, List[String])] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv").option("header", "true").load(path)

    //Dataset[(String, List[String])]
    return df.select("orderid", "eventname", "starttime", "endtime", "status")
      .filter(df("status").isin(List("Completed"):_*))  //filtering - only the Completed traces
      .orderBy("starttime")
      .map(x=>(x.get(0).toString,x.get(1).toString))
      .groupByKey(x=>x._1)
      .mapGroups{case(k, iter) => (k, iter.map(x => x._2).toList)}  //toList in order to keep the order of the events
  }

  /**
    * We assume that the events list contains no duplicates and they are sorted
    * If the events are A,B,C,D,E then pairs for computation are
    * AA, AB AC AD
    * BB BC BD
    * AC AD
    * CD
    * @param events
    * @return
    */
  def constructPairsForComputationFromEvents(events: List[String]): List[Pair] = {
    for {
      (x, idxX) <- events.zipWithIndex
      (y, idxY) <- events.zipWithIndex
      if (idxX == idxY || idxX < idxY)
    } yield new Pair(x,y)
  }

  /**
    * Not needed. Just left there in case of future use
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

  /**
    * Filter initial trace set. Keep only unique traces and not duplicates.
    * Also, filter out those traces, which have frequency less than percentage variable.
    * @param tracesDS
    * @param percentage
    * @return
    */
  def filterTraces(tracesDS: Dataset[(String, List[String])], percentage: Float): Dataset[(String, List[String])] = {
    implicit def listStringEncoder: org.apache.spark.sql.Encoder[List[String]] = org.apache.spark.sql.Encoders.kryo[List[String]]
    implicit def tupleListStringEncoder: org.apache.spark.sql.Encoder[(String, List[String])] = org.apache.spark.sql.Encoders.kryo[(String, List[String])]
    implicit def longStringEncoder: org.apache.spark.sql.Encoder[(List[String], Long)] = org.apache.spark.sql.Encoders.kryo[(List[String], Long)]
    implicit def floatStringEncoder: org.apache.spark.sql.Encoder[(List[String], Float)] = org.apache.spark.sql.Encoders.kryo[(List[String], Float)]

    val initNumberOfTraces = tracesDS.count()
    println("Initial number of traces = " + initNumberOfTraces)

    import org.apache.spark.sql.functions._
    val tracesToInspect = tracesDS
      .toDF("traceId", "trace")
      .groupBy("trace")
      .agg(count("trace"))
      .map(trace=>(trace.getAs[Seq[String]]("trace").toList,trace.getAs[Long]("count(trace)")))
      .filter(trace => (trace._2.toFloat / initNumberOfTraces) > (percentage/100) )
      .map(trace=>("xxx", trace._1))

    println("Number of traces to inspect = " + tracesToInspect.count())
    tracesToInspect
  }
}
