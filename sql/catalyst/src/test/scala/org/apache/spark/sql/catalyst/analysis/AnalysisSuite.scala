/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.analysis

<<<<<<< HEAD
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
=======
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.SimpleCatalystConf
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._

<<<<<<< HEAD
class AnalysisSuite extends SparkFunSuite with BeforeAndAfter {
=======
// todo: remove this and use AnalysisTest instead.
object AnalysisSuite {
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
  val caseSensitiveConf = new SimpleCatalystConf(true)
  val caseInsensitiveConf = new SimpleCatalystConf(false)

  val caseSensitiveCatalog = new SimpleCatalog(caseSensitiveConf)
  val caseInsensitiveCatalog = new SimpleCatalog(caseInsensitiveConf)

  val caseSensitiveAnalyzer =
    new Analyzer(caseSensitiveCatalog, EmptyFunctionRegistry, caseSensitiveConf) {
      override val extendedResolutionRules = EliminateSubQueries :: Nil
    }
  val caseInsensitiveAnalyzer =
    new Analyzer(caseInsensitiveCatalog, EmptyFunctionRegistry, caseInsensitiveConf) {
      override val extendedResolutionRules = EliminateSubQueries :: Nil
    }

  def caseSensitiveAnalyze(plan: LogicalPlan): Unit =
    caseSensitiveAnalyzer.checkAnalysis(caseSensitiveAnalyzer.execute(plan))

  def caseInsensitiveAnalyze(plan: LogicalPlan): Unit =
    caseInsensitiveAnalyzer.checkAnalysis(caseInsensitiveAnalyzer.execute(plan))

  val testRelation = LocalRelation(AttributeReference("a", IntegerType, nullable = true)())
  val testRelation2 = LocalRelation(
    AttributeReference("a", StringType)(),
    AttributeReference("b", StringType)(),
    AttributeReference("c", DoubleType)(),
    AttributeReference("d", DecimalType(10, 2))(),
    AttributeReference("e", ShortType)())

  val nestedRelation = LocalRelation(
    AttributeReference("top", StructType(
      StructField("duplicateField", StringType) ::
        StructField("duplicateField", StringType) ::
        StructField("differentCase", StringType) ::
        StructField("differentcase", StringType) :: Nil
    ))())

  val nestedRelation2 = LocalRelation(
    AttributeReference("top", StructType(
      StructField("aField", StringType) ::
        StructField("bField", StringType) ::
        StructField("cField", StringType) :: Nil
    ))())

  val listRelation = LocalRelation(
    AttributeReference("list", ArrayType(IntegerType))())

<<<<<<< HEAD
  before {
    caseSensitiveCatalog.registerTable(Seq("TaBlE"), testRelation)
    caseInsensitiveCatalog.registerTable(Seq("TaBlE"), testRelation)
  }
=======
  caseSensitiveCatalog.registerTable(Seq("TaBlE"), testRelation)
  caseInsensitiveCatalog.registerTable(Seq("TaBlE"), testRelation)
}


class AnalysisSuite extends AnalysisTest {
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

  test("union project *") {
    val plan = (1 to 100)
      .map(_ => testRelation)
      .fold[LogicalPlan](testRelation) { (a, b) =>
        a.select(UnresolvedStar(None)).select('a).unionAll(b.select(UnresolvedStar(None)))
      }

    assertAnalysisSuccess(plan)
  }

  test("check project's resolved") {
    assert(Project(testRelation.output, testRelation).resolved)

    assert(!Project(Seq(UnresolvedAttribute("a")), testRelation).resolved)

    val explode = Explode(AttributeReference("a", IntegerType, nullable = true)())
    assert(!Project(Seq(Alias(explode, "explode")()), testRelation).resolved)

    assert(!Project(Seq(Alias(Count(Literal(1)), "count")()), testRelation).resolved)
  }

  test("analyze project") {
    checkAnalysis(
      Project(Seq(UnresolvedAttribute("a")), testRelation),
      Project(testRelation.output, testRelation))

    checkAnalysis(
      Project(Seq(UnresolvedAttribute("TbL.a")), UnresolvedRelation(Seq("TaBlE"), Some("TbL"))),
      Project(testRelation.output, testRelation))

    assertAnalysisError(
      Project(Seq(UnresolvedAttribute("tBl.a")), UnresolvedRelation(Seq("TaBlE"), Some("TbL"))),
      Seq("cannot resolve"))

    checkAnalysis(
      Project(Seq(UnresolvedAttribute("TbL.a")), UnresolvedRelation(Seq("TaBlE"), Some("TbL"))),
      Project(testRelation.output, testRelation),
      caseSensitive = false)

    checkAnalysis(
      Project(Seq(UnresolvedAttribute("tBl.a")), UnresolvedRelation(Seq("TaBlE"), Some("TbL"))),
      Project(testRelation.output, testRelation),
      caseSensitive = false)
  }

  test("resolve relations") {
    assertAnalysisError(UnresolvedRelation(Seq("tAbLe"), None), Seq("Table Not Found: tAbLe"))

    checkAnalysis(UnresolvedRelation(Seq("TaBlE"), None), testRelation)

    checkAnalysis(UnresolvedRelation(Seq("tAbLe"), None), testRelation, caseSensitive = false)

    checkAnalysis(UnresolvedRelation(Seq("TaBlE"), None), testRelation, caseSensitive = false)
  }

<<<<<<< HEAD
  def errorTest(
      name: String,
      plan: LogicalPlan,
      errorMessages: Seq[String],
      caseSensitive: Boolean = true): Unit = {
    test(name) {
      val error = intercept[AnalysisException] {
        if (caseSensitive) {
          caseSensitiveAnalyze(plan)
        } else {
          caseInsensitiveAnalyze(plan)
        }
      }

      errorMessages.foreach(m => assert(error.getMessage.toLowerCase contains m.toLowerCase))
    }
  }

  errorTest(
    "unresolved window function",
    testRelation2.select(
      WindowExpression(
        UnresolvedWindowFunction(
          "lead",
          UnresolvedAttribute("c") :: Nil),
        WindowSpecDefinition(
          UnresolvedAttribute("a") :: Nil,
          SortOrder(UnresolvedAttribute("b"), Ascending) :: Nil,
          UnspecifiedFrame)).as('window)),
      "lead" :: "window functions currently requires a HiveContext" :: Nil)

  errorTest(
    "too many generators",
    listRelation.select(Explode('list).as('a), Explode('list).as('b)),
    "only one generator" :: "explode" :: Nil)

  errorTest(
    "unresolved attributes",
    testRelation.select('abcd),
    "cannot resolve" :: "abcd" :: Nil)

  errorTest(
    "bad casts",
    testRelation.select(Literal(1).cast(BinaryType).as('badCast)),
    "invalid cast" :: Literal(1).dataType.simpleString :: BinaryType.simpleString :: Nil)

  errorTest(
    "non-boolean filters",
    testRelation.where(Literal(1)),
    "filter" :: "'1'" :: "not a boolean" :: Literal(1).dataType.simpleString :: Nil)

  errorTest(
    "missing group by",
    testRelation2.groupBy('a)('b),
    "'b'" :: "group by" :: Nil
  )

  errorTest(
    "ambiguous field",
    nestedRelation.select($"top.duplicateField"),
    "Ambiguous reference to fields" :: "duplicateField" :: Nil,
    caseSensitive = false)

  errorTest(
    "ambiguous field due to case insensitivity",
    nestedRelation.select($"top.differentCase"),
    "Ambiguous reference to fields" :: "differentCase" :: "differentcase" :: Nil,
    caseSensitive = false)

  errorTest(
    "missing field",
    nestedRelation2.select($"top.c"),
    "No such struct field" :: "aField" :: "bField" :: "cField" :: Nil,
    caseSensitive = false)

  case class UnresolvedTestPlan() extends LeafNode {
    override lazy val resolved = false
    override def output: Seq[Attribute] = Nil
  }

  errorTest(
    "catch all unresolved plan",
    UnresolvedTestPlan(),
    "unresolved" :: Nil)


=======
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
  test("divide should be casted into fractional types") {
    val plan = caseInsensitiveAnalyzer.execute(
      testRelation2.select(
        'a / Literal(2) as 'div1,
        'a / 'b as 'div2,
        'a / 'c as 'div3,
        'a / 'd as 'div4,
        'e / 'e as 'div5))
    val pl = plan.asInstanceOf[Project].projectList

    assert(pl(0).dataType == DoubleType)
    assert(pl(1).dataType == DoubleType)
    assert(pl(2).dataType == DoubleType)
    // StringType will be promoted into Decimal(38, 18)
    assert(pl(3).dataType == DecimalType(38, 29))
    assert(pl(4).dataType == DoubleType)
  }

  test("pull out nondeterministic expressions from RepartitionByExpression") {
    val plan = RepartitionByExpression(Seq(Rand(33)), testRelation)
    val projected = Alias(Rand(33), "_nondeterministic")()
    val expected =
      Project(testRelation.output,
        RepartitionByExpression(Seq(projected.toAttribute),
          Project(testRelation.output :+ projected, testRelation)))
    checkAnalysis(plan, expected)
  }

  test("pull out nondeterministic expressions from Sort") {
    val plan = Sort(Seq(SortOrder(Rand(33), Ascending)), false, testRelation)
    val projected = Alias(Rand(33), "_nondeterministic")()
    val expected =
      Project(testRelation.output,
        Sort(Seq(SortOrder(projected.toAttribute, Ascending)), false,
          Project(testRelation.output :+ projected, testRelation)))
    checkAnalysis(plan, expected)
  }
}
