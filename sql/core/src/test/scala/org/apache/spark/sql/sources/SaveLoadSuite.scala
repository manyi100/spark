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

package org.apache.spark.sql.sources

import java.io.File

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.{AnalysisException, SaveMode, SQLConf, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class SaveLoadSuite extends DataSourceTest with BeforeAndAfterAll {

<<<<<<< HEAD
  import caseInsensitiveContext._
=======
  import caseInsensitiveContext.sql

  private lazy val sparkContext = caseInsensitiveContext.sparkContext
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

  var originalDefaultSource: String = null

  var path: File = null

  var df: DataFrame = null

  override def beforeAll(): Unit = {
    originalDefaultSource = caseInsensitiveContext.conf.defaultDataSourceName

    path = Utils.createTempDir()
    path.delete()

    val rdd = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str${i}"}"""))
<<<<<<< HEAD
    df = read.json(rdd)
=======
    df = caseInsensitiveContext.read.json(rdd)
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
    df.registerTempTable("jsonTable")
  }

  override def afterAll(): Unit = {
    caseInsensitiveContext.conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, originalDefaultSource)
  }

  after {
    caseInsensitiveContext.conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, originalDefaultSource)
    Utils.deleteRecursively(path)
  }

<<<<<<< HEAD
  def checkLoad(): Unit = {
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "org.apache.spark.sql.json")
    checkAnswer(read.load(path.toString), df.collect())

    // Test if we can pick up the data source name passed in load.
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "not a source name")
    checkAnswer(read.format("json").load(path.toString), df.collect())
    checkAnswer(read.format("json").load(path.toString), df.collect())
    val schema = StructType(StructField("b", StringType, true) :: Nil)
    checkAnswer(
      read.format("json").schema(schema).load(path.toString),
      sql("SELECT b FROM jsonTable").collect())
  }

  test("save with path and load") {
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "org.apache.spark.sql.json")
=======
  def checkLoad(expectedDF: DataFrame = df, tbl: String = "jsonTable"): Unit = {
    caseInsensitiveContext.conf.setConf(
      SQLConf.DEFAULT_DATA_SOURCE_NAME, "org.apache.spark.sql.json")
    checkAnswer(caseInsensitiveContext.read.load(path.toString), expectedDF.collect())

    // Test if we can pick up the data source name passed in load.
    caseInsensitiveContext.conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "not a source name")
    checkAnswer(caseInsensitiveContext.read.format("json").load(path.toString),
      expectedDF.collect())
    checkAnswer(caseInsensitiveContext.read.format("json").load(path.toString),
      expectedDF.collect())
    val schema = StructType(StructField("b", StringType, true) :: Nil)
    checkAnswer(
      caseInsensitiveContext.read.format("json").schema(schema).load(path.toString),
      sql(s"SELECT b FROM $tbl").collect())
  }

  test("save with path and load") {
    caseInsensitiveContext.conf.setConf(
      SQLConf.DEFAULT_DATA_SOURCE_NAME, "org.apache.spark.sql.json")
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
    df.write.save(path.toString)
    checkLoad()
  }

  test("save with string mode and path, and load") {
<<<<<<< HEAD
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "org.apache.spark.sql.json")
=======
    caseInsensitiveContext.conf.setConf(
      SQLConf.DEFAULT_DATA_SOURCE_NAME, "org.apache.spark.sql.json")
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
    path.createNewFile()
    df.write.mode("overwrite").save(path.toString)
    checkLoad()
  }

  test("save with path and datasource, and load") {
<<<<<<< HEAD
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "not a source name")
=======
    caseInsensitiveContext.conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "not a source name")
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
    df.write.json(path.toString)
    checkLoad()
  }

  test("save with data source and options, and load") {
<<<<<<< HEAD
    conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "not a source name")
=======
    caseInsensitiveContext.conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "not a source name")
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
    df.write.mode(SaveMode.ErrorIfExists).json(path.toString)
    checkLoad()
  }

  test("save and save again") {
    df.write.json(path.toString)

<<<<<<< HEAD
    var message = intercept[RuntimeException] {
=======
    val message = intercept[AnalysisException] {
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
      df.write.json(path.toString)
    }.getMessage

    assert(
      message.contains("already exists"),
      "We should complain that the path already exists.")

    if (path.exists()) Utils.deleteRecursively(path)

    df.write.json(path.toString)
    checkLoad()

    df.write.mode(SaveMode.Overwrite).json(path.toString)
    checkLoad()

<<<<<<< HEAD
    message = intercept[RuntimeException] {
      df.write.mode(SaveMode.Append).json(path.toString)
    }.getMessage
=======
    // verify the append mode
    df.write.mode(SaveMode.Append).json(path.toString)
    val df2 = df.unionAll(df)
    df2.registerTempTable("jsonTable2")
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

    checkLoad(df2, "jsonTable2")
  }
}
