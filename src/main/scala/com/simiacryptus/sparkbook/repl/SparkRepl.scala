/*
 * Copyright (c) 2018 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.simiacryptus.sparkbook.repl

import java.lang.reflect.InvocationTargetException
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.simiacryptus.sparkbook.Java8Util._
import com.simiacryptus.util.io.StringQuery.SimpleStringQuery
import com.simiacryptus.util.io.{MarkdownNotebookOutput, NotebookOutput}
import com.simiacryptus.util.lang.SerializableConsumer
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox
object SparkRepl {
  val spark = SparkSession.builder().getOrCreate()
}

import com.simiacryptus.sparkbook.repl.SparkRepl._

abstract class SparkRepl extends SerializableConsumer[NotebookOutput]() {

  @transient private lazy val toolbox = currentMirror.mkToolBox()

  def eval_scala(code: String) = {
    toolbox.eval(toolbox.parse(code)).asInstanceOf[Object]
  }

  def eval(code: String) = {
    val strings = code.split("\n")
    val interpreter = strings.head.trim
    val innercode = strings.tail.mkString("\n")
    val scalaPreamble =
      """
        |import org.apache.spark._
        |import org.apache.spark.sql._
        |import com.simiacryptus.sparkbook.repl.SparkRepl._
      """.stripMargin
    interpreter match {
      case "%sql" =>
        val quoted = "\"\"\"" + innercode + "\"\"\""
        eval_scala(scalaPreamble +
          s"""
             |val result = spark.sql($quoted)
             |val rows = result.collect()
             |println(result.schema.toList.map(_.name).mkString(", "))
             |println(rows.map(_.toSeq.mkString(", ")).mkString("\\n"))
             |result.schema
             |""".stripMargin)
      case "%scala" =>
        eval_scala(scalaPreamble + innercode)
      case _ =>
        eval_scala(scalaPreamble + code)
    }
  }

  val defaultCmd =
    """%sql
      | SELECT * FROM guids
      |""".stripMargin
  val inputTimeout = 60

  override def accept(log: NotebookOutput): Unit = {
    init()
    while (true) {
      def code = {
        new SimpleStringQuery(log.asInstanceOf[MarkdownNotebookOutput]).print(
          defaultCmd).get(inputTimeout, TimeUnit.MINUTES)
      }

      try {
        log.eval(() => {
          eval(code)
        })
      } catch {
        case e: RuntimeException if (
          e.getCause == null
            || e.getCause.getCause == null
            || e.getCause.getCause.getMessage == null
            || !e.getCause.isInstanceOf[InvocationTargetException]
            || !e.getCause.getCause.getMessage.contains("shutdown")
          ) => // Do Nothing
      }
    }
  }

  def init() = {
    spark.sqlContext.createDataFrame(spark.sparkContext.range(0, 10000).map(i => {
      Row(UUID.randomUUID().toString)
    }), StructType(
      List(
        StructField("guid", StringType, false)
      )
    )).createOrReplaceTempView("guids")
  }
}
