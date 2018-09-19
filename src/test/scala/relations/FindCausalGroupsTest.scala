package relations

import misc.{CausalGroup, Pair, Relation}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
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

    assert(causalGroups.contains(new CausalGroup[String](Set("E"), Set("D"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("B"), Set("D"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("E"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("B"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("C"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("C"), Set("D"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("B", "E"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("C", "E"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("B", "E"), Set("D"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("C", "E"), Set("D"))))
    assert(causalGroups.size==10)
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

     assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("B"))))
     assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("C"))))
     assert(causalGroups.contains(new CausalGroup[String](Set("B"), Set("D"))))
     assert(causalGroups.contains(new CausalGroup[String](Set("C"), Set("D"))))
     assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("B", "C"))))
     assert(causalGroups.contains(new CausalGroup[String](Set("B", "C"), Set("D"))))

     //false assertions
     assert(!causalGroups.contains(new CausalGroup[String](Set("B"), Set("I"))))
     assert(!causalGroups.contains(new CausalGroup[String](Set("B", "R"), Set("D"))))

     assert(causalGroups.size==6)
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
}
