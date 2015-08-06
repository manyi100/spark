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

package org.apache.spark.sql.json

import java.io.ByteArrayOutputStream
<<<<<<< HEAD
import java.sql.Timestamp

import scala.collection.Map

import com.fasterxml.jackson.core._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateUtils
import org.apache.spark.sql.json.JacksonUtils.nextUntil
import org.apache.spark.sql.types._
=======

import com.fasterxml.jackson.core._

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.json.JacksonUtils.nextUntil
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

private[sql] object JacksonParser {
  def apply(
      json: RDD[String],
      schema: StructType,
<<<<<<< HEAD
      columnNameOfCorruptRecords: String): RDD[Row] = {
=======
      columnNameOfCorruptRecords: String): RDD[InternalRow] = {
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
    parseJson(json, schema, columnNameOfCorruptRecords)
  }

  /**
   * Parse the current token (and related children) according to a desired schema
   */
  private[sql] def convertField(
      factory: JsonFactory,
      parser: JsonParser,
      schema: DataType): Any = {
    import com.fasterxml.jackson.core.JsonToken._
    (parser.getCurrentToken, schema) match {
      case (null | VALUE_NULL, _) =>
        null

      case (FIELD_NAME, _) =>
        parser.nextToken()
        convertField(factory, parser, schema)

      case (VALUE_STRING, StringType) =>
<<<<<<< HEAD
        UTF8String(parser.getText)
=======
        UTF8String.fromString(parser.getText)
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

      case (VALUE_STRING, _) if parser.getTextLength < 1 =>
        // guard the non string type
        null

      case (VALUE_STRING, DateType) =>
<<<<<<< HEAD
        DateUtils.millisToDays(DateUtils.stringToTime(parser.getText).getTime)

      case (VALUE_STRING, TimestampType) =>
        new Timestamp(DateUtils.stringToTime(parser.getText).getTime)

      case (VALUE_NUMBER_INT, TimestampType) =>
        new Timestamp(parser.getLongValue)
=======
        DateTimeUtils.millisToDays(DateTimeUtils.stringToTime(parser.getText).getTime)

      case (VALUE_STRING, TimestampType) =>
        DateTimeUtils.stringToTime(parser.getText).getTime * 1000L

      case (VALUE_NUMBER_INT, TimestampType) =>
        parser.getLongValue * 1000L
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

      case (_, StringType) =>
        val writer = new ByteArrayOutputStream()
        val generator = factory.createGenerator(writer, JsonEncoding.UTF8)
        generator.copyCurrentStructure(parser)
        generator.close()
<<<<<<< HEAD
        UTF8String(writer.toByteArray)
=======
        UTF8String.fromBytes(writer.toByteArray)
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT, FloatType) =>
        parser.getFloatValue

      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT, DoubleType) =>
        parser.getDoubleValue

<<<<<<< HEAD
      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT, DecimalType()) =>
        // TODO: add fixed precision and scale handling
        Decimal(parser.getDecimalValue)
=======
      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT, dt: DecimalType) =>
        Decimal(parser.getDecimalValue, dt.precision, dt.scale)
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

      case (VALUE_NUMBER_INT, ByteType) =>
        parser.getByteValue

      case (VALUE_NUMBER_INT, ShortType) =>
        parser.getShortValue

      case (VALUE_NUMBER_INT, IntegerType) =>
        parser.getIntValue

      case (VALUE_NUMBER_INT, LongType) =>
        parser.getLongValue

      case (VALUE_TRUE, BooleanType) =>
        true

      case (VALUE_FALSE, BooleanType) =>
        false

      case (START_OBJECT, st: StructType) =>
        convertObject(factory, parser, st)

<<<<<<< HEAD
      case (START_ARRAY, ArrayType(st, _)) =>
        convertList(factory, parser, st)
=======
      case (START_ARRAY, st: StructType) =>
        // SPARK-3308: support reading top level JSON arrays and take every element
        // in such an array as a row
        convertArray(factory, parser, st)

      case (START_ARRAY, ArrayType(st, _)) =>
        convertArray(factory, parser, st)
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

      case (START_OBJECT, ArrayType(st, _)) =>
        // the business end of SPARK-3308:
        // when an object is found but an array is requested just wrap it in a list
        convertField(factory, parser, st) :: Nil

      case (START_OBJECT, MapType(StringType, kt, _)) =>
        convertMap(factory, parser, kt)

      case (_, udt: UserDefinedType[_]) =>
<<<<<<< HEAD
        udt.deserialize(convertField(factory, parser, udt.sqlType))
=======
        convertField(factory, parser, udt.sqlType)
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
    }
  }

  /**
   * Parse an object from the token stream into a new Row representing the schema.
   *
   * Fields in the json that are not defined in the requested schema will be dropped.
   */
<<<<<<< HEAD
  private def convertObject(factory: JsonFactory, parser: JsonParser, schema: StructType): Row = {
=======
  private def convertObject(
      factory: JsonFactory,
      parser: JsonParser,
      schema: StructType): InternalRow = {
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
    val row = new GenericMutableRow(schema.length)
    while (nextUntil(parser, JsonToken.END_OBJECT)) {
      schema.getFieldIndex(parser.getCurrentName) match {
        case Some(index) =>
          row.update(index, convertField(factory, parser, schema(index).dataType))

        case None =>
          parser.skipChildren()
      }
    }

    row
  }

  /**
   * Parse an object as a Map, preserving all fields
   */
  private def convertMap(
      factory: JsonFactory,
      parser: JsonParser,
<<<<<<< HEAD
      valueType: DataType): Map[UTF8String, Any] = {
    val builder = Map.newBuilder[UTF8String, Any]
    while (nextUntil(parser, JsonToken.END_OBJECT)) {
      builder += UTF8String(parser.getCurrentName) -> convertField(factory, parser, valueType)
    }

    builder.result()
  }

  private def convertList(
      factory: JsonFactory,
      parser: JsonParser,
      schema: DataType): Seq[Any] = {
    val builder = Seq.newBuilder[Any]
    while (nextUntil(parser, JsonToken.END_ARRAY)) {
      builder += convertField(factory, parser, schema)
    }

    builder.result()
=======
      valueType: DataType): MapData = {
    val keys = ArrayBuffer.empty[UTF8String]
    val values = ArrayBuffer.empty[Any]
    while (nextUntil(parser, JsonToken.END_OBJECT)) {
      keys += UTF8String.fromString(parser.getCurrentName)
      values += convertField(factory, parser, valueType)
    }
    ArrayBasedMapData(keys.toArray, values.toArray)
  }

  private def convertArray(
      factory: JsonFactory,
      parser: JsonParser,
      elementType: DataType): ArrayData = {
    val values = ArrayBuffer.empty[Any]
    while (nextUntil(parser, JsonToken.END_ARRAY)) {
      values += convertField(factory, parser, elementType)
    }

    new GenericArrayData(values.toArray)
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
  }

  private def parseJson(
      json: RDD[String],
      schema: StructType,
<<<<<<< HEAD
      columnNameOfCorruptRecords: String): RDD[Row] = {

    def failedRecord(record: String): Seq[Row] = {
=======
      columnNameOfCorruptRecords: String): RDD[InternalRow] = {

    def failedRecord(record: String): Seq[InternalRow] = {
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
      // create a row even if no corrupt record column is present
      val row = new GenericMutableRow(schema.length)
      for (corruptIndex <- schema.getFieldIndex(columnNameOfCorruptRecords)) {
        require(schema(corruptIndex).dataType == StringType)
<<<<<<< HEAD
        row.update(corruptIndex, UTF8String(record))
=======
        row.update(corruptIndex, UTF8String.fromString(record))
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
      }

      Seq(row)
    }

    json.mapPartitions { iter =>
      val factory = new JsonFactory()

      iter.flatMap { record =>
        try {
          val parser = factory.createParser(record)
          parser.nextToken()

<<<<<<< HEAD
          // to support both object and arrays (see SPARK-3308) we'll start
          // by converting the StructType schema to an ArrayType and let
          // convertField wrap an object into a single value array when necessary.
          convertField(factory, parser, ArrayType(schema)) match {
            case null => failedRecord(record)
            case list: Seq[Row @unchecked] => list
=======
          convertField(factory, parser, schema) match {
            case null => failedRecord(record)
            case row: InternalRow => row :: Nil
            case array: ArrayData =>
              if (array.numElements() == 0) {
                Nil
              } else {
                array.toArray[InternalRow](schema)
              }
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
            case _ =>
              sys.error(
                s"Failed to parse record $record. Please make sure that each line of the file " +
                  "(or each string in the RDD) is a valid JSON object or an array of JSON objects.")
          }
        } catch {
          case _: JsonProcessingException =>
            failedRecord(record)
        }
      }
    }
  }
}
