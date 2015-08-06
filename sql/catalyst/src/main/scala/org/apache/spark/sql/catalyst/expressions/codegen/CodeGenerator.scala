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

package org.apache.spark.sql.catalyst.expressions.codegen

import scala.collection.mutable
import scala.language.existentials

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.codehaus.janino.ClassBodyEvaluator

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.PlatformDependent
import org.apache.spark.unsafe.types._


// These classes are here to avoid issues with serialization and integration with quasiquotes.
class IntegerHashSet extends org.apache.spark.util.collection.OpenHashSet[Int]
class LongHashSet extends org.apache.spark.util.collection.OpenHashSet[Long]

/**
 * Java source for evaluating an [[Expression]] given a [[InternalRow]] of input.
 *
 * @param code The sequence of statements required to evaluate the expression.
 * @param isNull A term that holds a boolean value representing whether the expression evaluated
 *                 to null.
 * @param primitive A term for a possible primitive value of the result of the evaluation. Not
 *                      valid if `isNull` is set to `true`.
 */
case class GeneratedExpressionCode(var code: String, var isNull: String, var primitive: String)

/**
 * A context for codegen, which is used to bookkeeping the expressions those are not supported
 * by codegen, then they are evaluated directly. The unsupported expression is appended at the
 * end of `references`, the position of it is kept in the code, used to access and evaluate it.
 */
class CodeGenContext {

  /**
   * Holding all the expressions those do not support codegen, will be evaluated directly.
   */
  val references: mutable.ArrayBuffer[Expression] = new mutable.ArrayBuffer[Expression]()

  /**
   * Holding expressions' mutable states like `MonotonicallyIncreasingID.count` as a
   * 3-tuple: java type, variable name, code to init it.
   * As an example, ("int", "count", "count = 0;") will produce code:
   * {{{
   *   private int count;
   * }}}
   * as a member variable, and add
   * {{{
   *   count = 0;
   * }}}
   * to the constructor.
   *
   * They will be kept as member variables in generated classes like `SpecificProjection`.
   */
  val mutableStates: mutable.ArrayBuffer[(String, String, String)] =
    mutable.ArrayBuffer.empty[(String, String, String)]

  def addMutableState(javaType: String, variableName: String, initCode: String): Unit = {
    mutableStates += ((javaType, variableName, initCode))
  }

  /**
   * Holding all the functions those will be added into generated class.
   */
  val addedFuntions: mutable.Map[String, String] =
    mutable.Map.empty[String, String]

  def addNewFunction(funcName: String, funcCode: String): Unit = {
    addedFuntions += ((funcName, funcCode))
  }

  final val JAVA_BOOLEAN = "boolean"
  final val JAVA_BYTE = "byte"
  final val JAVA_SHORT = "short"
  final val JAVA_INT = "int"
  final val JAVA_LONG = "long"
  final val JAVA_FLOAT = "float"
  final val JAVA_DOUBLE = "double"

  private val curId = new java.util.concurrent.atomic.AtomicInteger()

  /**
   * Returns a term name that is unique within this instance of a `CodeGenerator`.
   *
   * (Since we aren't in a macro context we do not seem to have access to the built in `freshName`
   * function.)
   */
  def freshName(prefix: String): String = {
    s"$prefix${curId.getAndIncrement}"
  }

  /**
   * Returns the code to access a value in `SpecializedGetters` for a given DataType.
   */
  def getValue(getter: String, dataType: DataType, ordinal: String): String = {
    val jt = javaType(dataType)
    dataType match {
      case _ if isPrimitiveType(jt) => s"$getter.get${primitiveTypeName(jt)}($ordinal)"
      case t: DecimalType => s"$getter.getDecimal($ordinal, ${t.precision}, ${t.scale})"
      case StringType => s"$getter.getUTF8String($ordinal)"
      case BinaryType => s"$getter.getBinary($ordinal)"
      case CalendarIntervalType => s"$getter.getInterval($ordinal)"
      case t: StructType => s"$getter.getStruct($ordinal, ${t.size})"
      case _: ArrayType => s"$getter.getArray($ordinal)"
      case _: MapType => s"$getter.getMap($ordinal)"
      case NullType => "null"
      case _ => s"($jt)$getter.get($ordinal, null)"
    }
  }

  /**
   * Returns the code to update a column in Row for a given DataType.
   */
  def setColumn(row: String, dataType: DataType, ordinal: Int, value: String): String = {
    val jt = javaType(dataType)
    dataType match {
      case _ if isPrimitiveType(jt) => s"$row.set${primitiveTypeName(jt)}($ordinal, $value)"
      case t: DecimalType => s"$row.setDecimal($ordinal, $value, ${t.precision})"
      // The UTF8String may came from UnsafeRow, otherwise clone is cheap (re-use the bytes)
      case StringType => s"$row.update($ordinal, $value.clone())"
      case _ => s"$row.update($ordinal, $value)"
    }
  }

  /**
   * Returns the name used in accessor and setter for a Java primitive type.
   */
  def primitiveTypeName(jt: String): String = jt match {
    case JAVA_INT => "Int"
    case _ => boxedType(jt)
  }

  def primitiveTypeName(dt: DataType): String = primitiveTypeName(javaType(dt))

<<<<<<< HEAD
    val inputTuple = newTermName(s"i")

    // TODO: Skip generation of null handling code when expression are not nullable.
    val primitiveEvaluation: PartialFunction[Expression, Seq[Tree]] = {
      case b @ BoundReference(ordinal, dataType, nullable) =>
        val nullValue = q"$inputTuple.isNullAt($ordinal)"
        q"""
          val $nullTerm: Boolean = $nullValue
          val $primitiveTerm: ${termForType(dataType)} =
            if($nullTerm)
              ${defaultPrimitive(dataType)}
            else
              ${getColumn(inputTuple, dataType, ordinal)}
         """.children

      case expressions.Literal(null, dataType) =>
        q"""
          val $nullTerm = true
          val $primitiveTerm: ${termForType(dataType)} = null.asInstanceOf[${termForType(dataType)}]
         """.children

      case expressions.Literal(value: Boolean, dataType) =>
        q"""
          val $nullTerm = ${value == null}
          val $primitiveTerm: ${termForType(dataType)} = $value
         """.children

      case expressions.Literal(value: UTF8String, dataType) =>
        q"""
          val $nullTerm = ${value == null}
          val $primitiveTerm: ${termForType(dataType)} =
            org.apache.spark.sql.types.UTF8String(${value.getBytes})
         """.children

      case expressions.Literal(value: Int, dataType) =>
        q"""
          val $nullTerm = ${value == null}
          val $primitiveTerm: ${termForType(dataType)} = $value
         """.children

      case expressions.Literal(value: Long, dataType) =>
        q"""
          val $nullTerm = ${value == null}
          val $primitiveTerm: ${termForType(dataType)} = $value
         """.children

      case Cast(e @ BinaryType(), StringType) =>
        val eval = expressionEvaluator(e)
        eval.code ++
        q"""
          val $nullTerm = ${eval.nullTerm}
          val $primitiveTerm =
            if($nullTerm)
              ${defaultPrimitive(StringType)}
            else
              org.apache.spark.sql.types.UTF8String(${eval.primitiveTerm}.asInstanceOf[Array[Byte]])
        """.children

      case Cast(child @ DateType(), StringType) =>
        child.castOrNull(c =>
          q"""org.apache.spark.sql.types.UTF8String(
                org.apache.spark.sql.catalyst.util.DateUtils.toString($c))""",
          StringType)

      case Cast(child @ NumericType(), IntegerType) =>
        child.castOrNull(c => q"$c.toInt", IntegerType)

      case Cast(child @ NumericType(), LongType) =>
        child.castOrNull(c => q"$c.toLong", LongType)

      case Cast(child @ NumericType(), DoubleType) =>
        child.castOrNull(c => q"$c.toDouble", DoubleType)

      case Cast(child @ NumericType(), FloatType) =>
        child.castOrNull(c => q"$c.toFloat", FloatType)

      // Special handling required for timestamps in hive test cases since the toString function
      // does not match the expected output.
      case Cast(e, StringType) if e.dataType != TimestampType =>
        val eval = expressionEvaluator(e)
        eval.code ++
        q"""
          val $nullTerm = ${eval.nullTerm}
          val $primitiveTerm =
            if($nullTerm)
              ${defaultPrimitive(StringType)}
            else
              org.apache.spark.sql.types.UTF8String(${eval.primitiveTerm}.toString)
        """.children

      case EqualTo(e1 @ BinaryType(), e2 @ BinaryType()) =>
        (e1, e2).evaluateAs (BooleanType) {
          case (eval1, eval2) =>
            q"""
              java.util.Arrays.equals($eval1.asInstanceOf[Array[Byte]],
                 $eval2.asInstanceOf[Array[Byte]])
            """
        }
=======
  /**
   * Returns the Java type for a DataType.
   */
  def javaType(dt: DataType): String = dt match {
    case BooleanType => JAVA_BOOLEAN
    case ByteType => JAVA_BYTE
    case ShortType => JAVA_SHORT
    case IntegerType | DateType => JAVA_INT
    case LongType | TimestampType => JAVA_LONG
    case FloatType => JAVA_FLOAT
    case DoubleType => JAVA_DOUBLE
    case dt: DecimalType => "Decimal"
    case BinaryType => "byte[]"
    case StringType => "UTF8String"
    case CalendarIntervalType => "CalendarInterval"
    case _: StructType => "InternalRow"
    case _: ArrayType => "ArrayData"
    case _: MapType => "MapData"
    case dt: OpenHashSetUDT if dt.elementType == IntegerType => classOf[IntegerHashSet].getName
    case dt: OpenHashSetUDT if dt.elementType == LongType => classOf[LongHashSet].getName
    case _ => "Object"
  }
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

  /**
   * Returns the boxed type in Java.
   */
  def boxedType(jt: String): String = jt match {
    case JAVA_BOOLEAN => "Boolean"
    case JAVA_BYTE => "Byte"
    case JAVA_SHORT => "Short"
    case JAVA_INT => "Integer"
    case JAVA_LONG => "Long"
    case JAVA_FLOAT => "Float"
    case JAVA_DOUBLE => "Double"
    case other => other
  }

  def boxedType(dt: DataType): String = boxedType(javaType(dt))

  /**
   * Returns the representation of default value for a given Java Type.
   */
  def defaultValue(jt: String): String = jt match {
    case JAVA_BOOLEAN => "false"
    case JAVA_BYTE => "(byte)-1"
    case JAVA_SHORT => "(short)-1"
    case JAVA_INT => "-1"
    case JAVA_LONG => "-1L"
    case JAVA_FLOAT => "-1.0f"
    case JAVA_DOUBLE => "-1.0"
    case _ => "null"
  }

<<<<<<< HEAD
        val funcName = newTermName(s"isIn${curId.getAndIncrement()}")

        q"""
            def $funcName: Boolean = {
              ..${eval.code}
              if(${eval.nullTerm}) return false
              ..$checks
              return false
            }
            val $nullTerm = false
            val $primitiveTerm = $funcName
        """.children
      */

      case GreaterThan(e1 @ NumericType(), e2 @ NumericType()) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => q"$eval1 > $eval2" }
      case GreaterThanOrEqual(e1 @ NumericType(), e2 @ NumericType()) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => q"$eval1 >= $eval2" }
      case LessThan(e1 @ NumericType(), e2 @ NumericType()) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => q"$eval1 < $eval2" }
      case LessThanOrEqual(e1 @ NumericType(), e2 @ NumericType()) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => q"$eval1 <= $eval2" }

      case And(e1, e2) =>
        val eval1 = expressionEvaluator(e1)
        val eval2 = expressionEvaluator(e2)

        q"""
          ..${eval1.code}
          var $nullTerm = false
          var $primitiveTerm: ${termForType(BooleanType)} = false

          if (!${eval1.nullTerm} && ${eval1.primitiveTerm} == false) {
          } else {
            ..${eval2.code}
            if (!${eval2.nullTerm} && ${eval2.primitiveTerm} == false) {
            } else if (!${eval1.nullTerm} && !${eval2.nullTerm}) {
              $primitiveTerm = true
            } else {
              $nullTerm = true
            }
          }
         """.children

      case Or(e1, e2) =>
        val eval1 = expressionEvaluator(e1)
        val eval2 = expressionEvaluator(e2)

        q"""
          ..${eval1.code}
          var $nullTerm = false
          var $primitiveTerm: ${termForType(BooleanType)} = false

          if (!${eval1.nullTerm} && ${eval1.primitiveTerm}) {
            $primitiveTerm = true
          } else {
            ..${eval2.code}
            if (!${eval2.nullTerm} && ${eval2.primitiveTerm}) {
              $primitiveTerm = true
            } else if (!${eval1.nullTerm} && !${eval2.nullTerm}) {
              $primitiveTerm = false
            } else {
              $nullTerm = true
            }
          }
         """.children

      case Not(child) =>
        // Uh, bad function name...
        child.castOrNull(c => q"!$c", BooleanType)

      case Add(e1, e2) => (e1, e2) evaluate { case (eval1, eval2) => q"$eval1 + $eval2" }
      case Subtract(e1, e2) => (e1, e2) evaluate { case (eval1, eval2) => q"$eval1 - $eval2" }
      case Multiply(e1, e2) => (e1, e2) evaluate { case (eval1, eval2) => q"$eval1 * $eval2" }
      case Divide(e1, e2) =>
        val eval1 = expressionEvaluator(e1)
        val eval2 = expressionEvaluator(e2)

        eval1.code ++ eval2.code ++
        q"""
          var $nullTerm = false
          var $primitiveTerm: ${termForType(e1.dataType)} = 0

          if (${eval1.nullTerm} || ${eval2.nullTerm} ) {
            $nullTerm = true
          } else if (${eval2.primitiveTerm} == 0)
            $nullTerm = true
          else {
            $primitiveTerm = ${eval1.primitiveTerm} / ${eval2.primitiveTerm}
          }
         """.children

      case Remainder(e1, e2) =>
        val eval1 = expressionEvaluator(e1)
        val eval2 = expressionEvaluator(e2)

        eval1.code ++ eval2.code ++
        q"""
          var $nullTerm = false
          var $primitiveTerm: ${termForType(e1.dataType)} = 0

          if (${eval1.nullTerm} || ${eval2.nullTerm} ) {
            $nullTerm = true
          } else if (${eval2.primitiveTerm} == 0)
            $nullTerm = true
          else {
            $nullTerm = false
            $primitiveTerm = ${eval1.primitiveTerm} % ${eval2.primitiveTerm}
          }
         """.children

      case IsNotNull(e) =>
        val eval = expressionEvaluator(e)
        q"""
          ..${eval.code}
          var $nullTerm = false
          var $primitiveTerm: ${termForType(BooleanType)} = !${eval.nullTerm}
        """.children

      case IsNull(e) =>
        val eval = expressionEvaluator(e)
        q"""
          ..${eval.code}
          var $nullTerm = false
          var $primitiveTerm: ${termForType(BooleanType)} = ${eval.nullTerm}
        """.children

      case c @ Coalesce(children) =>
        q"""
          var $nullTerm = true
          var $primitiveTerm: ${termForType(c.dataType)} = ${defaultPrimitive(c.dataType)}
        """.children ++
        children.map { c =>
          val eval = expressionEvaluator(c)
          q"""
            if($nullTerm) {
              ..${eval.code}
              if(!${eval.nullTerm}) {
                $nullTerm = false
                $primitiveTerm = ${eval.primitiveTerm}
              }
            }
          """
        }
=======
  def defaultValue(dt: DataType): String = defaultValue(javaType(dt))
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

  /**
   * Generates code for equal expression in Java.
   */
  def genEqual(dataType: DataType, c1: String, c2: String): String = dataType match {
    case BinaryType => s"java.util.Arrays.equals($c1, $c2)"
    case FloatType => s"(java.lang.Float.isNaN($c1) && java.lang.Float.isNaN($c2)) || $c1 == $c2"
    case DoubleType => s"(java.lang.Double.isNaN($c1) && java.lang.Double.isNaN($c2)) || $c1 == $c2"
    case dt: DataType if isPrimitiveType(dt) => s"$c1 == $c2"
    case other => s"$c1.equals($c2)"
  }

  /**
   * Generates code for comparing two expressions.
   *
   * @param dataType data type of the expressions
   * @param c1 name of the variable of expression 1's output
   * @param c2 name of the variable of expression 2's output
   */
  def genComp(dataType: DataType, c1: String, c2: String): String = dataType match {
    // java boolean doesn't support > or < operator
    case BooleanType => s"($c1 == $c2 ? 0 : ($c1 ? 1 : -1))"
    case DoubleType => s"org.apache.spark.util.Utils.nanSafeCompareDoubles($c1, $c2)"
    case FloatType => s"org.apache.spark.util.Utils.nanSafeCompareFloats($c1, $c2)"
    // use c1 - c2 may overflow
    case dt: DataType if isPrimitiveType(dt) => s"($c1 > $c2 ? 1 : $c1 < $c2 ? -1 : 0)"
    case BinaryType => s"org.apache.spark.sql.catalyst.util.TypeUtils.compareBinary($c1, $c2)"
    case NullType => "0"
    case schema: StructType =>
      val comparisons = GenerateOrdering.genComparisons(this, schema)
      val compareFunc = freshName("compareStruct")
      val funcCode: String =
        s"""
          public int $compareFunc(InternalRow a, InternalRow b) {
            InternalRow i = null;
            $comparisons
            return 0;
          }
        """
      addNewFunction(compareFunc, funcCode)
      s"this.$compareFunc($c1, $c2)"
    case other if other.isInstanceOf[AtomicType] => s"$c1.compare($c2)"
    case _ =>
      throw new IllegalArgumentException("cannot generate compare code for un-comparable type")
  }

  /**
   * List of java data types that have special accessors and setters in [[InternalRow]].
   */
  val primitiveTypes =
    Seq(JAVA_BOOLEAN, JAVA_BYTE, JAVA_SHORT, JAVA_INT, JAVA_LONG, JAVA_FLOAT, JAVA_DOUBLE)

  /**
   * Returns true if the Java type has a special accessor and setter in [[InternalRow]].
   */
  def isPrimitiveType(jt: String): Boolean = primitiveTypes.contains(jt)

  def isPrimitiveType(dt: DataType): Boolean = isPrimitiveType(javaType(dt))
}

/**
 * A wrapper for generated class, defines a `generate` method so that we can pass extra objects
 * into generated class.
 */
abstract class GeneratedClass {
  def generate(expressions: Array[Expression]): Any
}

/**
 * A base class for generators of byte code to perform expression evaluation.  Includes a set of
 * helpers for referring to Catalyst types and building trees that perform evaluation of individual
 * expressions.
 */
abstract class CodeGenerator[InType <: AnyRef, OutType <: AnyRef] extends Logging {

  protected val exprType: String = classOf[Expression].getName
  protected val mutableRowType: String = classOf[MutableRow].getName
  protected val genericMutableRowType: String = classOf[GenericMutableRow].getName

  protected def declareMutableStates(ctx: CodeGenContext): String = {
    ctx.mutableStates.map { case (javaType, variableName, _) =>
      s"private $javaType $variableName;"
    }.mkString
  }

  protected def initMutableStates(ctx: CodeGenContext): String = {
    ctx.mutableStates.map(_._3).mkString
  }

  protected def declareAddedFunctions(ctx: CodeGenContext): String = {
    ctx.addedFuntions.map { case (funcName, funcCode) => funcCode }.mkString
  }

  /**
   * Generates a class for a given input expression.  Called when there is not cached code
   * already available.
   */
  protected def create(in: InType): OutType

  /**
   * Canonicalizes an input expression. Used to avoid double caching expressions that differ only
   * cosmetically.
   */
  protected def canonicalize(in: InType): InType

  /** Binds an input expression to a given input schema */
  protected def bind(in: InType, inputSchema: Seq[Attribute]): InType

<<<<<<< HEAD
  protected def defaultPrimitive(dt: DataType) = dt match {
    case BooleanType => ru.Literal(Constant(false))
    case FloatType => ru.Literal(Constant(-1.0.toFloat))
    case StringType => q"""org.apache.spark.sql.types.UTF8String("<uninit>")"""
    case ShortType => ru.Literal(Constant(-1.toShort))
    case LongType => ru.Literal(Constant(-1L))
    case ByteType => ru.Literal(Constant(-1.toByte))
    case DoubleType => ru.Literal(Constant(-1.toDouble))
    case DecimalType() => q"org.apache.spark.sql.types.Decimal(-1)"
    case IntegerType => ru.Literal(Constant(-1))
    case DateType => ru.Literal(Constant(-1))
    case _ => ru.Literal(Constant(null))
=======
  /**
   * Compile the Java source code into a Java class, using Janino.
   */
  protected def compile(code: String): GeneratedClass = {
    cache.get(code)
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
  }

  /**
   * Compile the Java source code into a Java class, using Janino.
   */
  private[this] def doCompile(code: String): GeneratedClass = {
    val evaluator = new ClassBodyEvaluator()
    evaluator.setParentClassLoader(getClass.getClassLoader)
    evaluator.setDefaultImports(Array(
      classOf[PlatformDependent].getName,
      classOf[InternalRow].getName,
      classOf[UnsafeRow].getName,
      classOf[UTF8String].getName,
      classOf[Decimal].getName,
      classOf[CalendarInterval].getName,
      classOf[ArrayData].getName,
      classOf[UnsafeArrayData].getName,
      classOf[MapData].getName,
      classOf[UnsafeMapData].getName
    ))
    evaluator.setExtendedClass(classOf[GeneratedClass])
    try {
      evaluator.cook(code)
    } catch {
      case e: Exception =>
        val msg = s"failed to compile: $e\n" + CodeFormatter.format(code)
        logError(msg, e)
        throw new Exception(msg, e)
    }
    evaluator.getClazz().newInstance().asInstanceOf[GeneratedClass]
  }

  /**
   * A cache of generated classes.
   *
   * From the Guava Docs: A Cache is similar to ConcurrentMap, but not quite the same. The most
   * fundamental difference is that a ConcurrentMap persists all elements that are added to it until
   * they are explicitly removed. A Cache on the other hand is generally configured to evict entries
   * automatically, in order to constrain its memory footprint.  Note that this cache does not use
   * weak keys/values and thus does not respond to memory pressure.
   */
  private val cache = CacheBuilder.newBuilder()
    .maximumSize(100)
    .build(
      new CacheLoader[String, GeneratedClass]() {
        override def load(code: String): GeneratedClass = {
          val startTime = System.nanoTime()
          val result = doCompile(code)
          val endTime = System.nanoTime()
          def timeMs: Double = (endTime - startTime).toDouble / 1000000
          logInfo(s"Code generated in $timeMs ms")
          result
        }
      })

  /** Generates the requested evaluator binding the given expression(s) to the inputSchema. */
  def generate(expressions: InType, inputSchema: Seq[Attribute]): OutType =
    generate(bind(expressions, inputSchema))

  /** Generates the requested evaluator given already bound expression(s). */
  def generate(expressions: InType): OutType = create(canonicalize(expressions))

  /**
   * Create a new codegen context for expression evaluator, used to store those
   * expressions that don't support codegen
   */
  def newCodeGenContext(): CodeGenContext = {
    new CodeGenContext
  }
}
