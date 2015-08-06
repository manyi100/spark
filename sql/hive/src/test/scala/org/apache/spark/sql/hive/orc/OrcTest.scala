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

package org.apache.spark.sql.hive.orc

import java.io.File

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

<<<<<<< HEAD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql._

private[sql] trait OrcTest extends SQLTestUtils {
  protected def hiveContext = sqlContext.asInstanceOf[HiveContext]

  import sqlContext.sparkContext
  import sqlContext.implicits._
=======
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql._
import org.apache.spark.sql.test.SQLTestUtils

private[sql] trait OrcTest extends SQLTestUtils { this: SparkFunSuite =>
  lazy val sqlContext = org.apache.spark.sql.hive.test.TestHive

  import sqlContext.implicits._
  import sqlContext.sparkContext
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c

  /**
   * Writes `data` to a Orc file, which is then passed to `f` and will be deleted after `f`
   * returns.
   */
  protected def withOrcFile[T <: Product: ClassTag: TypeTag]
      (data: Seq[T])
      (f: String => Unit): Unit = {
    withTempPath { file =>
<<<<<<< HEAD
      sparkContext.parallelize(data).toDF().write.format("orc").save(file.getCanonicalPath)
=======
      sparkContext.parallelize(data).toDF().write.orc(file.getCanonicalPath)
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
      f(file.getCanonicalPath)
    }
  }

  /**
   * Writes `data` to a Orc file and reads it back as a [[DataFrame]],
   * which is then passed to `f`. The Orc file will be deleted after `f` returns.
   */
  protected def withOrcDataFrame[T <: Product: ClassTag: TypeTag]
      (data: Seq[T])
      (f: DataFrame => Unit): Unit = {
<<<<<<< HEAD
    withOrcFile(data)(path => f(hiveContext.read.format("orc").load(path)))
=======
    withOrcFile(data)(path => f(sqlContext.read.orc(path)))
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
  }

  /**
   * Writes `data` to a Orc file, reads it back as a [[DataFrame]] and registers it as a
   * temporary table named `tableName`, then call `f`. The temporary table together with the
   * Orc file will be dropped/deleted after `f` returns.
   */
  protected def withOrcTable[T <: Product: ClassTag: TypeTag]
      (data: Seq[T], tableName: String)
      (f: => Unit): Unit = {
    withOrcDataFrame(data) { df =>
<<<<<<< HEAD
      hiveContext.registerDataFrameAsTable(df, tableName)
=======
      sqlContext.registerDataFrameAsTable(df, tableName)
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
      withTempTable(tableName)(f)
    }
  }

  protected def makeOrcFile[T <: Product: ClassTag: TypeTag](
      data: Seq[T], path: File): Unit = {
<<<<<<< HEAD
    data.toDF().write.format("orc").mode(SaveMode.Overwrite).save(path.getCanonicalPath)
=======
    data.toDF().write.mode(SaveMode.Overwrite).orc(path.getCanonicalPath)
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
  }

  protected def makeOrcFile[T <: Product: ClassTag: TypeTag](
      df: DataFrame, path: File): Unit = {
<<<<<<< HEAD
    df.write.format("orc").mode(SaveMode.Overwrite).save(path.getCanonicalPath)
=======
    df.write.mode(SaveMode.Overwrite).orc(path.getCanonicalPath)
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
  }
}
