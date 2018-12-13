package steps

import misc.{CausalGroup, Pair}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class FindMaximalPairsTest extends FunSuite {

  implicit def pairEncoder: org.apache.spark.sql.Encoder[Pair] = org.apache.spark.sql.Encoders.kryo[Pair]
  implicit def causalGroupGenericEncoder: org.apache.spark.sql.Encoder[CausalGroup[String]] = org.apache.spark.sql.Encoders.kryo[CausalGroup[String]]
  implicit def setEncoder: org.apache.spark.sql.Encoder[Set[String]] = org.apache.spark.sql.Encoders.kryo[Set[String]]

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .appName("AlphaAlgorithm-FindCausalGroupsTest")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    .getOrCreate()

  import spark.implicits._

  test("Check FindMaximalPairs correct functionality - Log 1") {
    val causalGroups = Seq(
      new CausalGroup[String](Set("E"), Set("D")),
      new CausalGroup[String](Set("B"), Set("D")),
      new CausalGroup[String](Set("A"), Set("E")),
      new CausalGroup[String](Set("A"), Set("B")),
      new CausalGroup[String](Set("A"), Set("C")),
      new CausalGroup[String](Set("C"), Set("D")),
      new CausalGroup[String](Set("A"), Set("B", "E")),
      new CausalGroup[String](Set("A"), Set("C", "E")),
      new CausalGroup[String](Set("B", "E"), Set("D")),
      new CausalGroup[String](Set("C", "E"), Set("D"))
    ).toDS()

    val findMaximalPairs: FindMaximalPairs = new FindMaximalPairs(causalGroups)
    val onlyMaximalGroups = findMaximalPairs.extract()

    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("A"), Set("B", "E"))))
    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("A"), Set("C", "E"))))
    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("B", "E"), Set("D"))))
    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("C", "E"), Set("D"))))
    assert(onlyMaximalGroups.size==4)
  }

  test("Check FindMaximalPairs correct functionality - Log 2") {
    val causalGroups = List(
      new CausalGroup[String](Set("A"), Set("B")),
      new CausalGroup[String](Set("A"), Set("C")),
      new CausalGroup[String](Set("B"), Set("D")),
      new CausalGroup[String](Set("C"), Set("D")),
      new CausalGroup[String](Set("A"), Set("B", "C")),
      new CausalGroup[String](Set("B", "C"), Set("D"))
    ).toDS()

    val findMaximalPairs: FindMaximalPairs = new FindMaximalPairs(causalGroups)
    val onlyMaximalGroups = findMaximalPairs.extract()

    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("A"), Set("B", "C"))))
    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("B", "C"), Set("D"))))
    assert(onlyMaximalGroups.size==2)
  }

  test("Check FindMaximalPairs correct functionality - Log 3") {
    val causalGroups = List(
      new CausalGroup[String](Set("A"), Set("B")),
      new CausalGroup[String](Set("A"), Set("C")),
      new CausalGroup[String](Set("A"), Set("B","C")),
      new CausalGroup[String](Set("B"), Set("D")),
      new CausalGroup[String](Set("B"), Set("E")),
      new CausalGroup[String](Set("C"), Set("G")),
      new CausalGroup[String](Set("F","G"), Set("H")),
      new CausalGroup[String](Set("D"), Set("F")),
      new CausalGroup[String](Set("E"), Set("F")),
      new CausalGroup[String](Set("F"), Set("H")),
      new CausalGroup[String](Set("G"), Set("H"))
    ).toDS()

    val findMaximalPairs: FindMaximalPairs = new FindMaximalPairs(causalGroups)
    val onlyMaximalGroups = findMaximalPairs.extract()

    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("A"), Set("B", "C"))))
    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("B"), Set("D"))))
    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("B"), Set("E"))))
    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("C"), Set("G"))))
    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("D"), Set("F"))))
    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("E"), Set("F"))))
    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("F", "G"), Set("H"))))
    assert(onlyMaximalGroups.size==7)
  }

  test("Check FindMaximalPairs correct functionality - Log 4") {
    val causalGroups = List(
      new CausalGroup[String](Set("C"), Set("E")),
      new CausalGroup[String](Set("C"), Set("D")),
      new CausalGroup[String](Set("E"), Set("B")),
      new CausalGroup[String](Set("A"), Set("B")),
      new CausalGroup[String](Set("B"), Set("C")),
      new CausalGroup[String](Set("A","E"), Set("B")),
      new CausalGroup[String](Set("C"), Set("E","D"))
    ).toDS()

    val findMaximalPairs: FindMaximalPairs = new FindMaximalPairs(causalGroups)
    val onlyMaximalGroups = findMaximalPairs.extract()

    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("C"), Set("D", "E"))))
    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("B"), Set("C"))))
    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("A", "E"), Set("B"))))
    assert(onlyMaximalGroups.size==3)
  }

  test("Check FindMaximalPairs correct functionality - Log 5") {
    val causalGroups = List(
      new CausalGroup[String](Set("A"), Set("B")),
      new CausalGroup[String](Set("A"), Set("D")),
      new CausalGroup[String](Set("A"), Set("B","D")),
      new CausalGroup[String](Set("B"), Set("C")),
      new CausalGroup[String](Set("C"), Set("E")),
      new CausalGroup[String](Set("D"), Set("E")),
      new CausalGroup[String](Set("C","D"), Set("E")),
      new CausalGroup[String](Set("E"), Set("F")),
      new CausalGroup[String](Set("E"), Set("G")),
      new CausalGroup[String](Set("F"), Set("H")),
      new CausalGroup[String](Set("G"), Set("H"))
    ).toDS()

    val findMaximalPairs: FindMaximalPairs = new FindMaximalPairs(causalGroups)
    val onlyMaximalGroups = findMaximalPairs.extract()

    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("A"), Set("B", "D"))))
    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("B"), Set("C"))))
    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("C", "D"), Set("E"))))
    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("E"), Set("F"))))
    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("E"), Set("G"))))
    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("F"), Set("H"))))
    assert(onlyMaximalGroups.contains(new CausalGroup[String](Set("G"), Set("H"))))
    assert(onlyMaximalGroups.size==7)
  }

  test("Check is subset functionality") {
    val groups_A1 = (new CausalGroup[String](Set("A"), Set("B")), new CausalGroup[String](Set("A"), Set("B", "E")))  //true
    val groups_A2 = (new CausalGroup[String](Set("A"), Set("B", "E")), new CausalGroup[String](Set("A"), Set("B")))  //false
    val groups_B1 = (new CausalGroup[String](Set("A"), Set("B")), new CausalGroup[String](Set("A"), Set("E"))) //false
    val groups_C1 = (new CausalGroup[String](Set("A"), Set("B", "C")), new CausalGroup[String](Set("A"), Set("E"))) //false
    val groups_D1 = (new CausalGroup[String](Set("A", "B"), Set("C")), new CausalGroup[String](Set("B"), Set("C")))  //false
    val groups_D2 = (new CausalGroup[String](Set("B"), Set("C")), new CausalGroup[String](Set("A", "B"), Set("C")))  //true
    val groups_E1 = (new CausalGroup[String](Set("A", "B", "C"), Set("D", "E")), new CausalGroup[String](Set("B"), Set("D")))  //false
    val groups_E2 = (new CausalGroup[String](Set("B"), Set("D")), new CausalGroup[String](Set("A", "B", "C"), Set("D", "E")))  //true
    val groups_G1 = (new CausalGroup[String](Set("A", "B", "C"), Set("D", "E")), new CausalGroup[String](Set("B"), Set("D", "F")))  //false
    val groups_G2 = (new CausalGroup[String](Set("B"), Set("D", "F")), new CausalGroup[String](Set("A", "B", "C"), Set("D", "E")))  //false
    val groups_H1 = (new CausalGroup[String](Set("A", "B", "C"), Set("E")), new CausalGroup[String](Set("B"), Set("E", "F")))  //false

    val findMaximalPairs: FindMaximalPairs = new FindMaximalPairs(null)

    assert(findMaximalPairs.isSubsetOf(groups_A1._1, groups_A1._2) == true)
    assert(findMaximalPairs.isSubsetOf(groups_A2._1, groups_A2._2) == false)
    assert(findMaximalPairs.isSubsetOf(groups_B1._1, groups_B1._2) == false)
    assert(findMaximalPairs.isSubsetOf(groups_C1._1, groups_C1._2) == false)
    assert(findMaximalPairs.isSubsetOf(groups_D1._1, groups_D1._2) == false)
    assert(findMaximalPairs.isSubsetOf(groups_D2._1, groups_D2._2) == true)
    assert(findMaximalPairs.isSubsetOf(groups_E1._1, groups_E1._2) == false)
    assert(findMaximalPairs.isSubsetOf(groups_E2._1, groups_E2._2) == true)
    assert(findMaximalPairs.isSubsetOf(groups_G1._1, groups_G1._2) == false)
    assert(findMaximalPairs.isSubsetOf(groups_G2._1, groups_G2._2) == false)
    assert(findMaximalPairs.isSubsetOf(groups_H1._1, groups_H1._2) == false)
  }

}
