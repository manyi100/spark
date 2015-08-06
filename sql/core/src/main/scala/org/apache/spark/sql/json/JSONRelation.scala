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

import java.io.CharArrayWriter

import com.fasterxml.jackson.core.JsonFactory
import com.google.common.base.Objects
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{Text, LongWritable, NullWritable}
import org.apache.hadoop.mapred.{JobConf, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext, Job}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.Logging
import org.apache.spark.mapred.SparkHadoopMapRedUtil

import org.apache.spark.rdd.RDD
<<<<<<< HEAD
import org.apache.spark.sql.catalyst.expressions.{Expression, Attribute, Row}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}


private[sql] class DefaultSource
  extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider {
=======
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionSpec
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

private[sql] class DefaultSource extends HadoopFsRelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      paths: Array[String],
      dataSchema: Option[StructType],
      partitionColumns: Option[StructType],
      parameters: Map[String, String]): HadoopFsRelation = {
    val samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)

<<<<<<< HEAD
    new JSONRelation(path, samplingRatio, None, sqlContext)
=======
    new JSONRelation(None, samplingRatio, dataSchema, None, partitionColumns, paths)(sqlContext)
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
  }
}

<<<<<<< HEAD
  /** Returns a new base relation with the given schema and parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    val path = checkPath(parameters)
    val samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)

    new JSONRelation(path, samplingRatio, Some(schema), sqlContext)
=======
private[sql] class JSONRelation(
    val inputRDD: Option[RDD[String]],
    val samplingRatio: Double,
    val maybeDataSchema: Option[StructType],
    val maybePartitionSpec: Option[PartitionSpec],
    override val userDefinedPartitionColumns: Option[StructType],
    override val paths: Array[String] = Array.empty[String])(@transient val sqlContext: SQLContext)
  extends HadoopFsRelation(maybePartitionSpec) {

  /** Constraints to be imposed on schema to be stored. */
  private def checkConstraints(schema: StructType): Unit = {
    if (schema.fieldNames.length != schema.fieldNames.distinct.length) {
      val duplicateColumns = schema.fieldNames.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => "\"" + x + "\""
      }.mkString(", ")
      throw new AnalysisException(s"Duplicate column(s) : $duplicateColumns found, " +
        s"cannot save to JSON format")
    }
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
  }

  override val needConversion: Boolean = false

  private def createBaseRdd(inputPaths: Array[FileStatus]): RDD[String] = {
    val job = new Job(sqlContext.sparkContext.hadoopConfiguration)
    val conf = job.getConfiguration

<<<<<<< HEAD
private[sql] class JSONRelation(
    // baseRDD is not immutable with respect to INSERT OVERWRITE
    // and so it must be recreated at least as often as the
    // underlying inputs are modified. To be safe, a function is
    // used instead of a regular RDD value to ensure a fresh RDD is
    // recreated for each and every operation.
    baseRDD: () => RDD[String],
    val path: Option[String],
    val samplingRatio: Double,
    userSpecifiedSchema: Option[StructType])(
    @transient val sqlContext: SQLContext)
  extends BaseRelation
  with TableScan
  with InsertableRelation
  with CatalystScan {

  def this(
      path: String,
      samplingRatio: Double,
      userSpecifiedSchema: Option[StructType],
      sqlContext: SQLContext) =
    this(
      () => sqlContext.sparkContext.textFile(path),
      Some(path),
      samplingRatio,
      userSpecifiedSchema)(sqlContext)

  private val useJacksonStreamingAPI: Boolean = sqlContext.conf.useJacksonStreamingAPI
=======
    val paths = inputPaths.map(_.getPath)

    if (paths.nonEmpty) {
      FileInputFormat.setInputPaths(job, paths: _*)
    }
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

    sqlContext.sparkContext.hadoopRDD(
      conf.asInstanceOf[JobConf],
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text]).map(_._2.toString) // get the text line
  }

<<<<<<< HEAD
  override lazy val schema = userSpecifiedSchema.getOrElse {
    if (useJacksonStreamingAPI) {
      InferSchema(
        baseRDD(),
        samplingRatio,
        sqlContext.conf.columnNameOfCorruptRecord)
    } else {
      JsonRDD.nullTypeToStringType(
        JsonRDD.inferSchema(
          baseRDD(),
          samplingRatio,
          sqlContext.conf.columnNameOfCorruptRecord))
    }
  }

  override def buildScan(): RDD[Row] = {
    if (useJacksonStreamingAPI) {
      JacksonParser(
        baseRDD(),
        schema,
        sqlContext.conf.columnNameOfCorruptRecord)
    } else {
      JsonRDD.jsonStringToRow(
        baseRDD(),
        schema,
        sqlContext.conf.columnNameOfCorruptRecord)
    }
  }

  override def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row] = {
    if (useJacksonStreamingAPI) {
      JacksonParser(
        baseRDD(),
        StructType.fromAttributes(requiredColumns),
        sqlContext.conf.columnNameOfCorruptRecord)
    } else {
      JsonRDD.jsonStringToRow(
        baseRDD(),
        StructType.fromAttributes(requiredColumns),
        sqlContext.conf.columnNameOfCorruptRecord)
    }
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val filesystemPath = path match {
      case Some(p) => new Path(p)
      case None =>
        throw new IOException(s"Cannot INSERT into table with no path defined")
    }

    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    if (overwrite) {
      if (fs.exists(filesystemPath)) {
        var success: Boolean = false
        try {
          success = fs.delete(filesystemPath, true)
        } catch {
          case e: IOException =>
            throw new IOException(
              s"Unable to clear output directory ${filesystemPath.toString} prior"
                + s" to writing to JSON table:\n${e.toString}")
        }
        if (!success) {
          throw new IOException(
            s"Unable to clear output directory ${filesystemPath.toString} prior"
              + s" to writing to JSON table.")
        }
      }
      // Write the data.
      data.toJSON.saveAsTextFile(filesystemPath.toString)
      // Right now, we assume that the schema is not changed. We will not update the schema.
      // schema = data.schema
    } else {
      // TODO: Support INSERT INTO
      sys.error("JSON table only support INSERT OVERWRITE for now.")
=======
  override lazy val dataSchema = {
    val jsonSchema = maybeDataSchema.getOrElse {
      val files = cachedLeafStatuses().filterNot { status =>
        val name = status.getPath.getName
        name.startsWith("_") || name.startsWith(".")
      }.toArray
      InferSchema(
        inputRDD.getOrElse(createBaseRdd(files)),
        samplingRatio,
        sqlContext.conf.columnNameOfCorruptRecord)
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
    }
    checkConstraints(jsonSchema)

    jsonSchema
  }

  override def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputPaths: Array[FileStatus]): RDD[Row] = {
    JacksonParser(
      inputRDD.getOrElse(createBaseRdd(inputPaths)),
      StructType(requiredColumns.map(dataSchema(_))),
      sqlContext.conf.columnNameOfCorruptRecord).asInstanceOf[RDD[Row]]
  }

  override def equals(other: Any): Boolean = other match {
    case that: JSONRelation =>
      ((inputRDD, that.inputRDD) match {
        case (Some(thizRdd), Some(thatRdd)) => thizRdd eq thatRdd
        case (None, None) => true
        case _ => false
      }) && paths.toSet == that.paths.toSet &&
        dataSchema == that.dataSchema &&
        schema == that.schema
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hashCode(
      inputRDD,
      paths.toSet,
      dataSchema,
      schema,
      partitionColumns)
  }

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new JsonOutputWriter(path, dataSchema, context)
      }
    }
  }
}

private[json] class JsonOutputWriter(
    path: String,
    dataSchema: StructType,
    context: TaskAttemptContext)
  extends OutputWriterInternal with SparkHadoopMapRedUtil with Logging {

  val writer = new CharArrayWriter()
  // create the Generator without separator inserted between 2 records
  val gen = new JsonFactory().createGenerator(writer).setRootValueSeparator(null)

  val result = new Text()

  private val recordWriter: RecordWriter[NullWritable, Text] = {
    new TextOutputFormat[NullWritable, Text]() {
      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        val uniqueWriteJobId = context.getConfiguration.get("spark.sql.sources.writeJobUUID")
        val split = context.getTaskAttemptID.getTaskID.getId
        new Path(path, f"part-r-$split%05d-$uniqueWriteJobId$extension")
      }
    }.getRecordWriter(context)
  }

  override def writeInternal(row: InternalRow): Unit = {
    JacksonGenerator(dataSchema, gen, row)
    gen.flush()

    result.set(writer.toString)
    writer.reset()

    recordWriter.write(NullWritable.get(), result)
  }

  override def close(): Unit = {
    gen.close()
    recordWriter.close(context)
  }
}
