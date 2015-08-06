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

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.errors
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.types.DataType

/**
 * Thrown when an invalid attempt is made to access a property of a tree that has yet to be fully
 * resolved.
 */
class UnresolvedException[TreeType <: TreeNode[_]](tree: TreeType, function: String) extends
  errors.TreeNodeException(tree, s"Invalid call to $function on unresolved object", null)

/**
 * Holds the name of a relation that has yet to be looked up in a [[Catalog]].
 */
case class UnresolvedRelation(
    tableIdentifier: Seq[String],
    alias: Option[String] = None) extends LeafNode {

  /** Returns a `.` separated name for this relation. */
  def tableName: String = tableIdentifier.mkString(".")

  override def output: Seq[Attribute] = Nil

  override lazy val resolved = false
}

/**
 * Holds the name of an attribute that has yet to be resolved.
 */
case class UnresolvedAttribute(nameParts: Seq[String]) extends Attribute with Unevaluable {

  def name: String =
    nameParts.map(n => if (n.contains(".")) s"`$n`" else n).mkString(".")

  override def exprId: ExprId = throw new UnresolvedException(this, "exprId")
  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override def qualifiers: Seq[String] = throw new UnresolvedException(this, "qualifiers")
  override lazy val resolved = false

  override def newInstance(): UnresolvedAttribute = this
  override def withNullability(newNullability: Boolean): UnresolvedAttribute = this
  override def withQualifiers(newQualifiers: Seq[String]): UnresolvedAttribute = this
  override def withName(newName: String): UnresolvedAttribute = UnresolvedAttribute.quoted(newName)

  override def toString: String = s"'$name"
}

object UnresolvedAttribute {
  def apply(name: String): UnresolvedAttribute = new UnresolvedAttribute(name.split("\\."))
  def quoted(name: String): UnresolvedAttribute = new UnresolvedAttribute(Seq(name))
}

case class UnresolvedFunction(
    name: String,
    children: Seq[Expression],
    isDistinct: Boolean)
  extends Expression with Unevaluable {

  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def foldable: Boolean = throw new UnresolvedException(this, "foldable")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override lazy val resolved = false

  override def toString: String = s"'$name(${children.mkString(",")})"
}

/**
 * Represents all of the input attributes to a given relational operator, for example in
 * "SELECT * FROM ...". A [[Star]] gets automatically expanded during analysis.
 */
<<<<<<< HEAD
trait Star extends NamedExpression with trees.LeafNode[Expression] {
  self: Product =>
=======
abstract class Star extends LeafExpression with NamedExpression {
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

  override def name: String = throw new UnresolvedException(this, "name")
  override def exprId: ExprId = throw new UnresolvedException(this, "exprId")
  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override def qualifiers: Seq[String] = throw new UnresolvedException(this, "qualifiers")
  override def toAttribute: Attribute = throw new UnresolvedException(this, "toAttribute")
  override lazy val resolved = false

<<<<<<< HEAD
  // Star gets expanded at runtime so we never evaluate a Star.
  override def eval(input: Row = null): EvaluatedType =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")

=======
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
  def expand(input: Seq[Attribute], resolver: Resolver): Seq[NamedExpression]
}


/**
 * Represents all of the input attributes to a given relational operator, for example in
 * "SELECT * FROM ...".
 *
 * @param table an optional table that should be the target of the expansion.  If omitted all
 *              tables' columns are produced.
 */
case class UnresolvedStar(table: Option[String]) extends Star with Unevaluable {

  override def expand(input: Seq[Attribute], resolver: Resolver): Seq[NamedExpression] = {
    val expandedAttributes: Seq[Attribute] = table match {
      // If there is no table specified, use all input attributes.
      case None => input
      // If there is a table, pick out attributes that are part of this table.
      case Some(t) => input.filter(_.qualifiers.filter(resolver(_, t)).nonEmpty)
    }
    expandedAttributes.zip(input).map {
      case (n: NamedExpression, _) => n
      case (e, originalAttribute) =>
        Alias(e, originalAttribute.name)(qualifiers = originalAttribute.qualifiers)
    }
  }

  override def toString: String = table.map(_ + ".").getOrElse("") + "*"
}

/**
 * Used to assign new names to Generator's output, such as hive udtf.
 * For example the SQL expression "stack(2, key, value, key, value) as (a, b)" could be represented
 * as follows:
 *  MultiAlias(stack_function, Seq(a, b))

 * @param child the computation being performed
 * @param names the names to be associated with each output of computing [[child]].
 */
case class MultiAlias(child: Expression, names: Seq[String])
<<<<<<< HEAD
  extends NamedExpression with trees.UnaryNode[Expression] {
=======
  extends UnaryExpression with NamedExpression with CodegenFallback {
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

  override def name: String = throw new UnresolvedException(this, "name")

  override def exprId: ExprId = throw new UnresolvedException(this, "exprId")

  override def dataType: DataType = throw new UnresolvedException(this, "dataType")

  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")

  override def qualifiers: Seq[String] = throw new UnresolvedException(this, "qualifiers")

  override def toAttribute: Attribute = throw new UnresolvedException(this, "toAttribute")

  override lazy val resolved = false
<<<<<<< HEAD

  override def eval(input: Row = null): EvaluatedType =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")
=======
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

  override def toString: String = s"$child AS $names"

}

/**
 * Represents all the resolved input attributes to a given relational operator. This is used
 * in the data frame DSL.
 *
 * @param expressions Expressions to expand.
 */
case class ResolvedStar(expressions: Seq[NamedExpression]) extends Star with Unevaluable {
  override def expand(input: Seq[Attribute], resolver: Resolver): Seq[NamedExpression] = expressions
  override def toString: String = expressions.mkString("ResolvedStar(", ", ", ")")
}

/**
 * Extracts a value or values from an Expression
 *
 * @param child The expression to extract value from,
 *              can be Map, Array, Struct or array of Structs.
 * @param extraction The expression to describe the extraction,
 *                   can be key of Map, index of Array, field name of Struct.
 */
case class UnresolvedExtractValue(child: Expression, extraction: Expression)
<<<<<<< HEAD
  extends UnaryExpression {
=======
  extends UnaryExpression with Unevaluable {
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def foldable: Boolean = throw new UnresolvedException(this, "foldable")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override lazy val resolved = false

  override def toString: String = s"$child[$extraction]"
}

<<<<<<< HEAD
  override def toString: String = s"$child[$extraction]"
=======
/**
 * Holds the expression that has yet to be aliased.
 */
case class UnresolvedAlias(child: Expression)
  extends UnaryExpression with NamedExpression with Unevaluable {

  override def toAttribute: Attribute = throw new UnresolvedException(this, "toAttribute")
  override def qualifiers: Seq[String] = throw new UnresolvedException(this, "qualifiers")
  override def exprId: ExprId = throw new UnresolvedException(this, "exprId")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def name: String = throw new UnresolvedException(this, "name")

  override lazy val resolved = false
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
}
