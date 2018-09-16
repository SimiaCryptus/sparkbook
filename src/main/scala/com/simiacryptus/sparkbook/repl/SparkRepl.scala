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
import java.util
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.simiacryptus.sparkbook.Java8Util._
import com.simiacryptus.util.TableOutput
import com.simiacryptus.util.io.StringQuery.SimpleStringQuery
import com.simiacryptus.util.io.{MarkdownNotebookOutput, NotebookOutput}
import com.simiacryptus.util.lang.{SerializableConsumer, SerializableFunction}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

object SparkRepl extends SparkSessionProvider {
  implicit var log: NotebookOutput = null

  def out(frame: DataFrame)(implicit log: NotebookOutput) = {
    def nameFn(n: String) = {
      var name = n
      //while (name.length < 3) name = "_" + name + "_"
      name
    }

    val tableOutput = new TableOutput()
    frame.schema.fields.foreach(f => {
      tableOutput.schema.put(nameFn(f.name), f.dataType match {
        case IntegerType => classOf[Integer]
        case StringType => classOf[String]
        case _ => classOf[Object]
      })
    })
    frame.limit(100).collect().foreach(row => {
      val rowData: util.HashMap[CharSequence, AnyRef] = new util.HashMap[CharSequence, AnyRef]()
      row.schema.fields.zipWithIndex.foreach(tuple => {
        val (field, fieldIndex) = tuple
        val value = row.get(fieldIndex)
        if (null != value) {
          rowData.put(nameFn(field.name), value.toString)
        }
      })
      tableOutput.putRow(rowData)
    })
    log.p(tableOutput.toMarkdownTable)
  }

}

class SparkRepl extends SerializableFunction[NotebookOutput,Object] with SparkSessionProvider {

  @transient private lazy val toolbox = currentMirror.mkToolBox()

  def eval_scala(code: String) = {
    toolbox.eval(toolbox.parse(
      """
        |import org.apache.spark._
        |import org.apache.spark.sql._
        |import com.simiacryptus.sparkbook.repl.SparkRepl._
      """.stripMargin + code)).asInstanceOf[Object]
  }

  val tripleQuote = "\"\"\""

  def eval(code: String): Object = {
    val strings = code.split("\n")
    val interpreter = strings.head.trim
    val innercode = strings.tail.mkString("\n")
    interpreter match {
      case "%sql" =>
        eval_sql(innercode)
      case "%scala" =>
        eval_scala(innercode)
      case _ =>
        eval_scala(code)
    }
  }

  def eval_sql(innercode: String): Object = {
    innercode.split("""(?<!\\);""").map(_.trim).filterNot(_.isEmpty).map(sql => {
      eval_scala(
        s"""
           |out(spark.sql($tripleQuote$sql$tripleQuote))
           |""".stripMargin.trim)
    })
  }

  val defaultCmd =
    """%sql
      | SELECT * FROM guids
      |""".stripMargin
  val inputTimeout = 60

  override def apply(log: NotebookOutput): Object = {
    SparkRepl.log = log
    init()

    def code = new SimpleStringQuery(log.asInstanceOf[MarkdownNotebookOutput])
      .print(defaultCmd).get(inputTimeout, TimeUnit.MINUTES)

    while (shouldContinue()) {
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
      } finally {
        log.write()
      }
    }
    null
  }

  def shouldContinue() = {
    true
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
