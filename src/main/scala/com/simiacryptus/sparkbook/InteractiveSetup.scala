package com.simiacryptus.sparkbook

import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.{MapperFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.simiacryptus.lang.SerializableFunction
import com.simiacryptus.notebook.{JsonQuery, MarkdownNotebookOutput, NotebookOutput}
import com.simiacryptus.sparkbook.util.Java8Util._

trait InteractiveSetup[T] extends SerializableFunction[NotebookOutput, T] {
  final override def apply(log: NotebookOutput): T = {
    val value = new JsonQuery[InteractiveSetup[T]](log.asInstanceOf[MarkdownNotebookOutput]).setMapper({
      new ObjectMapper()
        .enable(SerializationFeature.INDENT_OUTPUT)
        .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
        .enable(MapperFeature.USE_STD_BEAN_NAMING)
        .registerModule(DefaultScalaModule)
        .enableDefaultTyping()
    }).print(this).get(inputTimeoutSeconds, TimeUnit.SECONDS)
    Option(value).getOrElse(this).accept2(log)
  }

  def inputTimeoutSeconds = 60

  def accept2(l: NotebookOutput): T
}
