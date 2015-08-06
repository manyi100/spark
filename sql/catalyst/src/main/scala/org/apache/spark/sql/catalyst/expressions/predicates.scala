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

package org.apache.spark.sql.catalyst.expressions

import scala.collection.mutable

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenFallback, GeneratedExpressionCode, CodeGenContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


object InterpretedPredicate {
  def create(expression: Expression, inputSchema: Seq[Attribute]): (InternalRow => Boolean) =
    create(BindReferences.bindReference(expression, inputSchema))

  def create(expression: Expression): (InternalRow => Boolean) = {
    expression.foreach {
      case n: Nondeterministic => n.setInitialValues()
      case _ =>
    }
    (r: InternalRow) => expression.eval(r).asInstanceOf[Boolean]
  }
}


/**
 * An [[Expression]] that returns a boolean value.
 */
trait Predicate extends Expression {
  override def dataType: DataType = BooleanType
}


trait PredicateHelper {
  protected def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  protected def splitDisjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case Or(cond1, cond2) =>
        splitDisjunctivePredicates(cond1) ++ splitDisjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  /**
   * Returns true if `expr` can be evaluated using only the output of `plan`.  This method
   * can be used to determine when is is acceptable to move expression evaluation within a query
   * plan.
   *
   * For example consider a join between two relations R(a, b) and S(c, d).
   *
   * `canEvaluate(EqualTo(a,b), R)` returns `true` where as `canEvaluate(EqualTo(a,c), R)` returns
   * `false`.
   */
  protected def canEvaluate(expr: Expression, plan: LogicalPlan): Boolean =
    expr.references.subsetOf(plan.outputSet)
}


<<<<<<< HEAD
case class Not(child: Expression) extends UnaryExpression with Predicate with ExpectsInputTypes {
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable
  override def toString: String = s"NOT $child"

  override def expectedChildTypes: Seq[DataType] = Seq(BooleanType)

  override def eval(input: Row): Any = {
    child.eval(input) match {
      case null => null
      case b: Boolean => !b
    }
=======
case class Not(child: Expression)
  extends UnaryExpression with Predicate with ImplicitCastInputTypes {

  override def toString: String = s"NOT $child"

  override def inputTypes: Seq[DataType] = Seq(BooleanType)

  protected override def nullSafeEval(input: Any): Any = !input.asInstanceOf[Boolean]

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"!($c)")
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
  }
}


/**
 * Evaluates to `true` if `list` contains `value`.
 */
case class In(value: Expression, list: Seq[Expression]) extends Predicate
    with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = value.dataType +: list.map(_.dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (list.exists(l => l.dataType != value.dataType)) {
      TypeCheckResult.TypeCheckFailure(
        "Arguments must be same type")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def children: Seq[Expression] = value +: list

  override def nullable: Boolean = false // TODO: Figure out correct nullability semantics of IN.
  override def toString: String = s"$value IN ${list.mkString("(", ",", ")")}"

  override def eval(input: InternalRow): Any = {
    val evaluatedValue = value.eval(input)
    list.exists(e => e.eval(input) == evaluatedValue)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val valueGen = value.gen(ctx)
    val listGen = list.map(_.gen(ctx))
    val listCode = listGen.map(x =>
      s"""
        if (!${ev.primitive}) {
          ${x.code}
          if (${ctx.genEqual(value.dataType, valueGen.primitive, x.primitive)}) {
            ${ev.primitive} = true;
          }
        }
       """).mkString("\n")
    s"""
      ${valueGen.code}
      boolean ${ev.primitive} = false;
      boolean ${ev.isNull} = false;
      $listCode
    """
  }
}

/**
 * Optimized version of In clause, when all filter values of In clause are
 * static.
 */
case class InSet(child: Expression, hset: Set[Any]) extends UnaryExpression with Predicate {

  override def nullable: Boolean = false // TODO: Figure out correct nullability semantics of IN.
  override def toString: String = s"$child INSET ${hset.mkString("(", ",", ")")}"

<<<<<<< HEAD
  override def foldable: Boolean = value.foldable
  override def nullable: Boolean = true // TODO: Figure out correct nullability semantics of IN.
  override def toString: String = s"$value INSET ${hset.mkString("(", ",", ")")}"
=======
  override def eval(input: InternalRow): Any = {
    hset.contains(child.eval(input))
  }
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

  def getHSet(): Set[Any] = hset

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val setName = classOf[Set[Any]].getName
    val InSetName = classOf[InSet].getName
    val childGen = child.gen(ctx)
    ctx.references += this
    val hsetTerm = ctx.freshName("hset")
    ctx.addMutableState(setName, hsetTerm,
      s"$hsetTerm = (($InSetName)expressions[${ctx.references.size - 1}]).getHSet();")
    s"""
      ${childGen.code}
      boolean ${ev.isNull} = false;
      boolean ${ev.primitive} = $hsetTerm.contains(${childGen.primitive});
     """
  }
}

<<<<<<< HEAD
case class And(left: Expression, right: Expression)
  extends BinaryExpression with Predicate with ExpectsInputTypes {

  override def expectedChildTypes: Seq[DataType] = Seq(BooleanType, BooleanType)
=======
case class And(left: Expression, right: Expression) extends BinaryOperator with Predicate {

  override def inputType: AbstractDataType = BooleanType
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

  override def symbol: String = "&&"

  override def eval(input: InternalRow): Any = {
    val input1 = left.eval(input)
    if (input1 == false) {
       false
    } else {
      val input2 = right.eval(input)
      if (input2 == false) {
        false
      } else {
        if (input1 != null && input2 != null) {
          true
        } else {
          null
        }
      }
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)

    // The result should be `false`, if any of them is `false` whenever the other is null or not.
    s"""
      ${eval1.code}
      boolean ${ev.isNull} = false;
      boolean ${ev.primitive} = false;

      if (!${eval1.isNull} && !${eval1.primitive}) {
      } else {
        ${eval2.code}
        if (!${eval2.isNull} && !${eval2.primitive}) {
        } else if (!${eval1.isNull} && !${eval2.isNull}) {
          ${ev.primitive} = true;
        } else {
          ${ev.isNull} = true;
        }
      }
     """
  }
}

<<<<<<< HEAD
case class Or(left: Expression, right: Expression)
  extends BinaryExpression with Predicate with ExpectsInputTypes {

  override def expectedChildTypes: Seq[DataType] = Seq(BooleanType, BooleanType)
=======

case class Or(left: Expression, right: Expression) extends BinaryOperator with Predicate {

  override def inputType: AbstractDataType = BooleanType
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

  override def symbol: String = "||"

  override def eval(input: InternalRow): Any = {
    val input1 = left.eval(input)
    if (input1 == true) {
      true
    } else {
      val input2 = right.eval(input)
      if (input2 == true) {
        true
      } else {
        if (input1 != null && input2 != null) {
          false
        } else {
          null
        }
      }
    }
  }

<<<<<<< HEAD
abstract class BinaryComparison extends BinaryExpression with Predicate {
  self: Product =>
}

/** An extractor that matches both standard 3VL equality and null-safe equality. */
private[sql] object Equality {
  def unapply(e: BinaryComparison): Option[(Expression, Expression)] = e match {
    case EqualTo(l, r) => Some((l, r))
    case EqualNullSafe(l, r) => Some((l, r))
    case _ => None
  }
}

case class EqualTo(left: Expression, right: Expression) extends BinaryComparison {
  override def symbol: String = "="
=======
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)

    // The result should be `true`, if any of them is `true` whenever the other is null or not.
    s"""
      ${eval1.code}
      boolean ${ev.isNull} = false;
      boolean ${ev.primitive} = true;
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

      if (!${eval1.isNull} && ${eval1.primitive}) {
      } else {
        ${eval2.code}
        if (!${eval2.isNull} && ${eval2.primitive}) {
        } else if (!${eval1.isNull} && !${eval2.isNull}) {
          ${ev.primitive} = false;
        } else {
          ${ev.isNull} = true;
        }
      }
     """
  }
}


abstract class BinaryComparison extends BinaryOperator with Predicate {

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    if (ctx.isPrimitiveType(left.dataType)
        && left.dataType != BooleanType // java boolean doesn't support > or < operator
        && left.dataType != FloatType
        && left.dataType != DoubleType) {
      // faster version
      defineCodeGen(ctx, ev, (c1, c2) => s"$c1 $symbol $c2")
    } else {
      defineCodeGen(ctx, ev, (c1, c2) => s"${ctx.genComp(left.dataType, c1, c2)} $symbol 0")
    }
  }
}


private[sql] object BinaryComparison {
  def unapply(e: BinaryComparison): Option[(Expression, Expression)] = Some((e.left, e.right))
}


/** An extractor that matches both standard 3VL equality and null-safe equality. */
private[sql] object Equality {
  def unapply(e: BinaryComparison): Option[(Expression, Expression)] = e match {
    case EqualTo(l, r) => Some((l, r))
    case EqualNullSafe(l, r) => Some((l, r))
    case _ => None
  }
}


case class EqualTo(left: Expression, right: Expression) extends BinaryComparison {

  override def inputType: AbstractDataType = AnyDataType

  override def symbol: String = "="

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    if (left.dataType == FloatType) {
      Utils.nanSafeCompareFloats(input1.asInstanceOf[Float], input2.asInstanceOf[Float]) == 0
    } else if (left.dataType == DoubleType) {
      Utils.nanSafeCompareDoubles(input1.asInstanceOf[Double], input2.asInstanceOf[Double]) == 0
    } else if (left.dataType != BinaryType) {
      input1 == input2
    } else {
      java.util.Arrays.equals(input1.asInstanceOf[Array[Byte]], input2.asInstanceOf[Array[Byte]])
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c1, c2) => ctx.genEqual(left.dataType, c1, c2))
  }
}


case class EqualNullSafe(left: Expression, right: Expression) extends BinaryComparison {

  override def inputType: AbstractDataType = AnyDataType

  override def symbol: String = "<=>"

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val input1 = left.eval(input)
    val input2 = right.eval(input)
    if (input1 == null && input2 == null) {
      true
    } else if (input1 == null || input2 == null) {
      false
    } else {
      if (left.dataType == FloatType) {
        Utils.nanSafeCompareFloats(input1.asInstanceOf[Float], input2.asInstanceOf[Float]) == 0
      } else if (left.dataType == DoubleType) {
        Utils.nanSafeCompareDoubles(input1.asInstanceOf[Double], input2.asInstanceOf[Double]) == 0
      } else if (left.dataType != BinaryType) {
        input1 == input2
      } else {
        java.util.Arrays.equals(input1.asInstanceOf[Array[Byte]], input2.asInstanceOf[Array[Byte]])
      }
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val equalCode = ctx.genEqual(left.dataType, eval1.primitive, eval2.primitive)
    ev.isNull = "false"
    eval1.code + eval2.code + s"""
        boolean ${ev.primitive} = (${eval1.isNull} && ${eval2.isNull}) ||
           (!${eval1.isNull} && $equalCode);
      """
  }
}


case class LessThan(left: Expression, right: Expression) extends BinaryComparison {

  override def inputType: AbstractDataType = TypeCollection.Ordered

  override def symbol: String = "<"

  private lazy val ordering = TypeUtils.getInterpretedOrdering(left.dataType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = ordering.lt(input1, input2)
}


case class LessThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {

  override def inputType: AbstractDataType = TypeCollection.Ordered

  override def symbol: String = "<="

  private lazy val ordering = TypeUtils.getInterpretedOrdering(left.dataType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = ordering.lteq(input1, input2)
}

<<<<<<< HEAD
trait CaseWhenLike extends Expression {
  self: Product =>

  type EvaluatedType = Any

  // Note that `branches` are considered in consecutive pairs (cond, val), and the optional last
  // element is the value for the default catch-all case (if provided).
  // Hence, `branches` consists of at least two elements, and can have an odd or even length.
  def branches: Seq[Expression]

  @transient lazy val whenList =
    branches.sliding(2, 2).collect { case Seq(whenExpr, _) => whenExpr }.toSeq
  @transient lazy val thenList =
    branches.sliding(2, 2).collect { case Seq(_, thenExpr) => thenExpr }.toSeq
  val elseValue = if (branches.length % 2 == 0) None else Option(branches.last)

  // both then and else val should be considered.
  def valueTypes: Seq[DataType] = (thenList ++ elseValue).map(_.dataType)
  def valueTypesEqual: Boolean = valueTypes.distinct.size <= 1

  override def dataType: DataType = {
    if (!resolved) {
      throw new UnresolvedException(this, "cannot resolve due to differing types in some branches")
    }
    valueTypes.head
  }

  override def nullable: Boolean = {
    // If no value is nullable and no elseValue is provided, the whole statement defaults to null.
    thenList.exists(_.nullable) || (elseValue.map(_.nullable).getOrElse(true))
  }
}

// scalastyle:off
/**
 * Case statements of the form "CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END".
 * Refer to this link for the corresponding semantics:
 * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-ConditionalFunctions
 */
// scalastyle:on
case class CaseWhen(branches: Seq[Expression]) extends CaseWhenLike {

  // Use private[this] Array to speed up evaluation.
  @transient private[this] lazy val branchesArr = branches.toArray
=======
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

case class GreaterThan(left: Expression, right: Expression) extends BinaryComparison {

<<<<<<< HEAD
  override lazy val resolved: Boolean =
    childrenResolved &&
    whenList.forall(_.dataType == BooleanType) &&
    valueTypesEqual

  /** Written in imperative fashion for performance considerations. */
  override def eval(input: Row): Any = {
    val len = branchesArr.length
    var i = 0
    // If all branches fail and an elseVal is not provided, the whole statement
    // defaults to null, according to Hive's semantics.
    while (i < len - 1) {
      if (branchesArr(i).eval(input) == true) {
        return branchesArr(i + 1).eval(input)
      }
      i += 2
    }
    var res: Any = null
    if (i == len - 1) {
      res = branchesArr(i).eval(input)
    }
    return res
  }

  override def toString: String = {
    "CASE" + branches.sliding(2, 2).map {
      case Seq(cond, value) => s" WHEN $cond THEN $value"
      case Seq(elseValue) => s" ELSE $elseValue"
    }.mkString
  }
}

// scalastyle:off
/**
 * Case statements of the form "CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END".
 * Refer to this link for the corresponding semantics:
 * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-ConditionalFunctions
 */
// scalastyle:on
case class CaseKeyWhen(key: Expression, branches: Seq[Expression]) extends CaseWhenLike {

  // Use private[this] Array to speed up evaluation.
  @transient private[this] lazy val branchesArr = branches.toArray

  override def children: Seq[Expression] = key +: branches

  override lazy val resolved: Boolean =
    childrenResolved && valueTypesEqual

  /** Written in imperative fashion for performance considerations. */
  override def eval(input: Row): Any = {
    val evaluatedKey = key.eval(input)
    val len = branchesArr.length
    var i = 0
    // If all branches fail and an elseVal is not provided, the whole statement
    // defaults to null, according to Hive's semantics.
    while (i < len - 1) {
      if (equalNullSafe(evaluatedKey, branchesArr(i).eval(input))) {
        return branchesArr(i + 1).eval(input)
      }
      i += 2
    }
    var res: Any = null
    if (i == len - 1) {
      res = branchesArr(i).eval(input)
    }
    return res
  }

  private def equalNullSafe(l: Any, r: Any) = {
    if (l == null && r == null) {
      true
    } else if (l == null || r == null) {
      false
    } else {
      l == r
    }
  }

  override def toString: String = {
    s"CASE $key" + branches.sliding(2, 2).map {
      case Seq(cond, value) => s" WHEN $cond THEN $value"
      case Seq(elseValue) => s" ELSE $elseValue"
    }.mkString
  }
=======
  override def inputType: AbstractDataType = TypeCollection.Ordered

  override def symbol: String = ">"

  private lazy val ordering = TypeUtils.getInterpretedOrdering(left.dataType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = ordering.gt(input1, input2)
}


case class GreaterThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {

  override def inputType: AbstractDataType = TypeCollection.Ordered

  override def symbol: String = ">="

  private lazy val ordering = TypeUtils.getInterpretedOrdering(left.dataType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = ordering.gteq(input1, input2)
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
}
