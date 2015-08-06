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
package org.apache.spark.status.api.v1

<<<<<<< HEAD
import java.text.SimpleDateFormat
=======
import java.text.{ParseException, SimpleDateFormat}
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
import java.util.TimeZone
import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.Response
import javax.ws.rs.core.Response.Status

<<<<<<< HEAD
import scala.util.Try

private[v1] class SimpleDateParam(val originalValue: String) {
  val timestamp: Long = {
    SimpleDateParam.formats.collectFirst {
      case fmt if Try(fmt.parse(originalValue)).isSuccess =>
        fmt.parse(originalValue).getTime()
    }.getOrElse(
      throw new WebApplicationException(
        Response
          .status(Status.BAD_REQUEST)
          .entity("Couldn't parse date: " + originalValue)
          .build()
      )
    )
  }
}

private[v1] object SimpleDateParam {

  val formats: Seq[SimpleDateFormat] = {

    val gmtDay = new SimpleDateFormat("yyyy-MM-dd")
    gmtDay.setTimeZone(TimeZone.getTimeZone("GMT"))

    Seq(
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz"),
      gmtDay
    )
=======
private[v1] class SimpleDateParam(val originalValue: String) {

  val timestamp: Long = {
    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz")
    try {
      format.parse(originalValue).getTime()
    } catch {
      case _: ParseException =>
        val gmtDay = new SimpleDateFormat("yyyy-MM-dd")
        gmtDay.setTimeZone(TimeZone.getTimeZone("GMT"))
        try {
          gmtDay.parse(originalValue).getTime()
        } catch {
          case _: ParseException =>
            throw new WebApplicationException(
              Response
                .status(Status.BAD_REQUEST)
                .entity("Couldn't parse date: " + originalValue)
                .build()
            )
        }
    }
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c
  }
}
