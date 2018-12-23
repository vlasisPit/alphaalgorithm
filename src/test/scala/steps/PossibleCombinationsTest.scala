package steps

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

class PossibleCombinationsTest extends FunSuite {

  val spark = SparkSession
    .builder()
    .appName("AlphaAlgorithm-FindCausalGroupsTest")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    .getOrCreate()

  import spark.implicits._

  Logger.getLogger("org").setLevel(Level.ERROR)

  test("Extract possible combinations from 3 member list") {
    val groupEvents : Dataset[String] = List("B", "C", "E").toDS()

    val possibleCombinations : PossibleCombinations = new PossibleCombinations(groupEvents);
    val combinations : List[Set[String]] = possibleCombinations.extractAllPossibleCombinations();

    assert(combinations.contains(Set("E")))
    assert(combinations.contains(Set("C")))
    assert(combinations.contains(Set("C","E")))
    assert(combinations.contains(Set("B")))
    assert(combinations.contains(Set("B","E")))
    assert(combinations.contains(Set("B","C")))
    assert(combinations.contains(Set("B","C","E")))

    assert(combinations.length==7)
  }

  test("Extract possible combinations from 4 member list") {
    val groupEvents : Dataset[String] = List("B", "C", "D", "E").toDS()

    val possibleCombinations : PossibleCombinations = new PossibleCombinations(groupEvents);
    val combinations : List[Set[String]] = possibleCombinations.extractAllPossibleCombinations();

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

    assert(combinations.length==15)
  }

}
