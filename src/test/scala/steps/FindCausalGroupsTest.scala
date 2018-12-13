package steps

import misc.{CausalGroup, Pair, Relation}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * Unit tests to check step 4 of Alpha Algorithm (extract causal groups)
  * Unit tests above are not true unit tests, because a true unit test means you have complete control over every component
  * in the test. There can be no interaction with databases, REST calls, file systems, or even the system clock; everything
  * has to be "doubled" (e.g. mocked, stubbed, etc).
  * In this test, SparkSession is created, but this is necessary to test the proper functionality of dataset operations like
  * groupByKey and mapGroups
  */
class FindCausalGroupsTest extends FunSuite with BeforeAndAfter {

  implicit def pairEncoder: org.apache.spark.sql.Encoder[Pair] = org.apache.spark.sql.Encoders.kryo[Pair]
  implicit def causalGroupGenericEncoder: org.apache.spark.sql.Encoder[CausalGroup[String]] = org.apache.spark.sql.Encoders.kryo[CausalGroup[String]]
  implicit def setEncoder: org.apache.spark.sql.Encoder[Set[String]] = org.apache.spark.sql.Encoders.kryo[Set[String]]
  implicit def tuple2[A1, A2](
                               implicit e1: Encoder[A1],
                               e2: Encoder[A2]
                             ): Encoder[(A1,A2)] = Encoders.tuple[A1,A2](e1, e2)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .appName("AlphaAlgorithm-FindCausalGroupsTest")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    .getOrCreate()

  import spark.implicits._

  test("Check FindCausalGroups correct functionality - Log 1") {
    val logRelations = Seq(
      (new Pair("E", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "A"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "C"), Relation.PARALLELISM.toString),
      (new Pair("E", "D"), Relation.CAUSALITY.toString),
      (new Pair("B", "D"), Relation.CAUSALITY.toString),
      (new Pair("A", "E"), Relation.CAUSALITY.toString),
      (new Pair("D", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "B"), Relation.CAUSALITY.toString),
      (new Pair("C", "C"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "B"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "C"), Relation.CAUSALITY.toString),
      (new Pair("C", "D"), Relation.CAUSALITY.toString))
      .toDS();

    val findCausalGroups: FindCausalGroups = new FindCausalGroups(logRelations)
    val causalGroups = findCausalGroups.extractCausalGroups()

    assert(contains(causalGroups, new CausalGroup[String](Set("E"), Set("D"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("B"), Set("D"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("A"), Set("E"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("A"), Set("B"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("A"), Set("C"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("C"), Set("D"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("A"), Set("B", "E"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("A"), Set("C", "E"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("B", "E"), Set("D"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("C", "E"), Set("D"))))
    assert(causalGroups.count()==10)
  }

   test("Check FindCausalGroups correct functionality - Log 2") {
     val logRelations = Seq(
       (new Pair("A", "D"), Relation.NEVER_FOLLOW.toString),
       (new Pair("B", "D"), Relation.CAUSALITY.toString),
       (new Pair("B", "C"), Relation.NEVER_FOLLOW.toString),
       (new Pair("A", "A"), Relation.NEVER_FOLLOW.toString),
       (new Pair("C", "C"), Relation.NEVER_FOLLOW.toString),
       (new Pair("A", "C"), Relation.CAUSALITY.toString),
       (new Pair("D", "D"), Relation.NEVER_FOLLOW.toString),
       (new Pair("A", "B"), Relation.CAUSALITY.toString),
       (new Pair("B", "B"), Relation.NEVER_FOLLOW.toString),
       (new Pair("C", "D"), Relation.CAUSALITY.toString))
       .toDS();

     val findCausalGroups: FindCausalGroups = new FindCausalGroups(logRelations)
     val causalGroups = findCausalGroups.extractCausalGroups()

     assert(contains(causalGroups, new CausalGroup[String](Set("A"), Set("B"))))
     assert(contains(causalGroups, new CausalGroup[String](Set("A"), Set("C"))))
     assert(contains(causalGroups, new CausalGroup[String](Set("B"), Set("D"))))
     assert(contains(causalGroups, new CausalGroup[String](Set("C"), Set("D"))))
     assert(contains(causalGroups, new CausalGroup[String](Set("A"), Set("B", "C"))))
     assert(contains(causalGroups, new CausalGroup[String](Set("B", "C"), Set("D"))))

     //false assertions
     assert(!contains(causalGroups, new CausalGroup[String](Set("B"), Set("I"))))
     assert(!contains(causalGroups, new CausalGroup[String](Set("B", "R"), Set("D"))))

     assert(causalGroups.count()==6)
   }

  test("Check FindCausalGroups correct functionality - Log 3") {
    val logRelations = Seq(
      (new Pair("A", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "F"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "G"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "H"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "C"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "F"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "G"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "H"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "F"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "H"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "C"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "G"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "H"), Relation.NEVER_FOLLOW.toString),
      (new Pair("E", "G"), Relation.NEVER_FOLLOW.toString),
      (new Pair("E", "H"), Relation.NEVER_FOLLOW.toString),
      (new Pair("F", "G"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "E"), Relation.PARALLELISM.toString),
      (new Pair("A", "B"), Relation.CAUSALITY.toString),
      (new Pair("A", "C"), Relation.CAUSALITY.toString),
      (new Pair("B", "D"), Relation.CAUSALITY.toString),
      (new Pair("B", "E"), Relation.CAUSALITY.toString),
      (new Pair("C", "G"), Relation.CAUSALITY.toString),
      (new Pair("D", "F"), Relation.CAUSALITY.toString),
      (new Pair("E", "F"), Relation.CAUSALITY.toString),
      (new Pair("F", "H"), Relation.CAUSALITY.toString),
      (new Pair("G", "H"), Relation.CAUSALITY.toString)
    ).toDS();

    val findCausalGroups: FindCausalGroups = new FindCausalGroups(logRelations)
    val causalGroups = findCausalGroups.extractCausalGroups()

    assert(contains(causalGroups, new CausalGroup[String](Set("A"), Set("B"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("A"), Set("C"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("A"), Set("B","C"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("B"), Set("D"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("B"), Set("E"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("C"), Set("G"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("F","G"), Set("H"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("D"), Set("F"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("E"), Set("F"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("F"), Set("H"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("G"), Set("H"))))

    //false assertions
    assert(!contains(causalGroups, new CausalGroup[String](Set("B"), Set("I"))))
    assert(!contains(causalGroups, new CausalGroup[String](Set("B", "R"), Set("D"))))

    assert(causalGroups.count()==11)
  }


  test("Check FindCausalGroups correct functionality - Log 4") {
    val logRelations = Seq(
      (new Pair("A", "C"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("E", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "B"), Relation.CAUSALITY.toString),
      (new Pair("B", "C"), Relation.CAUSALITY.toString),
      (new Pair("C", "E"), Relation.CAUSALITY.toString),
      (new Pair("C", "D"), Relation.CAUSALITY.toString),
      (new Pair("E", "B"), Relation.CAUSALITY.toString)
    ).toDS();

    val findCausalGroups: FindCausalGroups = new FindCausalGroups(logRelations)
    val causalGroups = findCausalGroups.extractCausalGroups()

    assert(contains(causalGroups, new CausalGroup[String](Set("C"), Set("E"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("C"), Set("D"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("E"), Set("B"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("A"), Set("B"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("B"), Set("C"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("A","E"), Set("B"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("C"), Set("E","D"))))

    //false assertions
    assert(!contains(causalGroups, new CausalGroup[String](Set("B"), Set("I"))))
    assert(!contains(causalGroups, new CausalGroup[String](Set("B", "R"), Set("D"))))

    assert(causalGroups.count()==7)
  }

  test("Check FindCausalGroups correct functionality - Log 5") {
    val logRelations = Seq(
      (new Pair("A", "C"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "F"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "G"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "H"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "F"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "G"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "H"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "F"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "G"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "H"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "B"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "C"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "F"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "G"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "H"), Relation.NEVER_FOLLOW.toString),
      (new Pair("E", "H"), Relation.NEVER_FOLLOW.toString),
      (new Pair("F", "G"), Relation.PARALLELISM.toString),
      (new Pair("A", "B"), Relation.CAUSALITY.toString),
      (new Pair("A", "D"), Relation.CAUSALITY.toString),
      (new Pair("B", "C"), Relation.CAUSALITY.toString),
      (new Pair("C", "E"), Relation.CAUSALITY.toString),
      (new Pair("D", "E"), Relation.CAUSALITY.toString),
      (new Pair("E", "F"), Relation.CAUSALITY.toString),
      (new Pair("E", "G"), Relation.CAUSALITY.toString),
      (new Pair("F", "H"), Relation.CAUSALITY.toString),
      (new Pair("G", "H"), Relation.CAUSALITY.toString)
      )
      .toDS();

    val findCausalGroups: FindCausalGroups = new FindCausalGroups(logRelations)
    val causalGroups = findCausalGroups.extractCausalGroups()

    assert(contains(causalGroups, new CausalGroup[String](Set("A"), Set("B"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("A"), Set("D"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("A"), Set("B","D"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("B"), Set("C"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("C"), Set("E"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("D"), Set("E"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("C","D"), Set("E"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("E"), Set("F"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("E"), Set("G"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("F"), Set("H"))))
    assert(contains(causalGroups, new CausalGroup[String](Set("G"), Set("H"))))

    //false assertions
    assert(!contains(causalGroups, new CausalGroup[String](Set("B"), Set("I"))))
    assert(!contains(causalGroups, new CausalGroup[String](Set("B", "R"), Set("D"))))

    assert(causalGroups.count()==11)
  }

  test("Check if a NeverFollow Relation exists. All relations are never follow") {
    val logRelations = Seq(
      (new Pair("A", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "A"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "C"), Relation.NEVER_FOLLOW.toString))
      .toDS();

    val groupEvents: Set[String] = Set("B", "C", "E")
    val findCausalGroups: FindCausalGroups = new FindCausalGroups(logRelations)

    val allRelationsAreNeverFollow = findCausalGroups.allRelationsAreNeverFollow(groupEvents)
    assert(allRelationsAreNeverFollow==true)
  }

  test("Check if a NeverFollow Relation exists. There is one PARALLELISM relation") {
    val logRelations = Seq(
      (new Pair("A", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "E"), Relation.PARALLELISM.toString),
      (new Pair("A", "A"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "C"), Relation.NEVER_FOLLOW.toString))
      .toDS();

    val groupEvents: Set[String] = Set("B", "C", "E")
    val findCausalGroups: FindCausalGroups = new FindCausalGroups(logRelations)

    val allRelationsAreNeverFollow = findCausalGroups.allRelationsAreNeverFollow(groupEvents)
    assert(allRelationsAreNeverFollow==false)
  }

  test("Check if a NeverFollow Relation exists. All relations are never follow for 4 event") {
    val logRelations = Seq(
      (new Pair("A", "B"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "C"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "C"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "D"), Relation.NEVER_FOLLOW.toString))
      .toDS();

    val groupEvents: Set[String] = Set("A", "B", "C", "D")
    val findCausalGroups: FindCausalGroups = new FindCausalGroups(logRelations)

    val allRelationsAreNeverFollow = findCausalGroups.allRelationsAreNeverFollow(groupEvents)
    assert(allRelationsAreNeverFollow==true)
  }

  test("NeverFollow Relation is not valid. There is one PARALLELISM relation. Group must be broken") {
    val logRelations = Seq(
      (new Pair("A", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "C"), Relation.PARALLELISM.toString),
      (new Pair("A", "A"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "E"), Relation.NEVER_FOLLOW.toString))
      .toDS();

    val groupEvents: Set[String] = Set("B", "C", "E")
    val findCausalGroups: FindCausalGroups = new FindCausalGroups(logRelations)

    val newGroups = findCausalGroups.checkIfNeverFollowRelationIsValidAndBreakTheGroup(groupEvents)
    assert(newGroups.contains(Set("C", "E")))
    assert(newGroups.contains(Set("B", "E")))

    assert(newGroups.size==2)
  }

  test("NeverFollow Relation is valid. Group must not be broken") {
    val logRelations = Seq(
      (new Pair("A", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "C"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "A"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "E"), Relation.NEVER_FOLLOW.toString))
      .toDS();

    val groupEvents: Set[String] = Set("B", "C", "E")
    val findCausalGroups: FindCausalGroups = new FindCausalGroups(logRelations)

    val newGroups = findCausalGroups.checkIfNeverFollowRelationIsValidAndBreakTheGroup(groupEvents)
    assert(newGroups.contains(Set("B", "C", "E")))
    assert(newGroups.contains(Set("B", "C")))
    assert(newGroups.contains(Set("B", "E")))
    assert(newGroups.contains(Set("C", "E")))

    assert(newGroups.size==4)
  }

  def contains(pairs : Dataset[CausalGroup[String]], causalGroup: CausalGroup[String]): Boolean = {
    return pairs.filter(x=>x==causalGroup).count()!=0
  }
}
