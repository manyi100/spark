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

package org.apache.spark.sql

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkFunSuite
<<<<<<< HEAD
import org.apache.spark.sql.test.TestSQLContext

class SQLContextSuite extends SparkFunSuite with BeforeAndAfterAll {

  private val testSqlContext = TestSQLContext
  private val testSparkContext = TestSQLContext.sparkContext

  override def afterAll(): Unit = {
    SQLContext.setLastInstantiatedContext(testSqlContext)
=======

class SQLContextSuite extends SparkFunSuite with BeforeAndAfterAll {

  private lazy val ctx = org.apache.spark.sql.test.TestSQLContext

  override def afterAll(): Unit = {
    SQLContext.setLastInstantiatedContext(ctx)
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
  }

  test("getOrCreate instantiates SQLContext") {
    SQLContext.clearLastInstantiatedContext()
<<<<<<< HEAD
    val sqlContext = SQLContext.getOrCreate(testSparkContext)
    assert(sqlContext != null, "SQLContext.getOrCreate returned null")
    assert(SQLContext.getOrCreate(testSparkContext).eq(sqlContext),
=======
    val sqlContext = SQLContext.getOrCreate(ctx.sparkContext)
    assert(sqlContext != null, "SQLContext.getOrCreate returned null")
    assert(SQLContext.getOrCreate(ctx.sparkContext).eq(sqlContext),
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
      "SQLContext created by SQLContext.getOrCreate not returned by SQLContext.getOrCreate")
  }

  test("getOrCreate gets last explicitly instantiated SQLContext") {
    SQLContext.clearLastInstantiatedContext()
<<<<<<< HEAD
    val sqlContext = new SQLContext(testSparkContext)
    assert(SQLContext.getOrCreate(testSparkContext) != null,
      "SQLContext.getOrCreate after explicitly created SQLContext returned null")
    assert(SQLContext.getOrCreate(testSparkContext).eq(sqlContext),
=======
    val sqlContext = new SQLContext(ctx.sparkContext)
    assert(SQLContext.getOrCreate(ctx.sparkContext) != null,
      "SQLContext.getOrCreate after explicitly created SQLContext returned null")
    assert(SQLContext.getOrCreate(ctx.sparkContext).eq(sqlContext),
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
      "SQLContext.getOrCreate after explicitly created SQLContext did not return the context")
  }
}
