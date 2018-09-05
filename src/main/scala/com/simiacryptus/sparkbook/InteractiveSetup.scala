package com.simiacryptus.sparkbook

import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.{MapperFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.simiacryptus.sparkbook.Java8Util._
import com.simiacryptus.util.io.{JsonQuery, MarkdownNotebookOutput, NotebookOutput}
import com.simiacryptus.util.lang.SerializableConsumer

trait InteractiveSetup extends SerializableConsumer[NotebookOutput] {
  def inputTimeoutSeconds = 60

  final override def accept(log: NotebookOutput) = {
    val value = new JsonQuery[InteractiveSetup](log.asInstanceOf[MarkdownNotebookOutput]).setMapper({
      new ObjectMapper()
        .enable(SerializationFeature.INDENT_OUTPUT)
        .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
        .enable(MapperFeature.USE_STD_BEAN_NAMING)
        .registerModule(DefaultScalaModule)
        .enableDefaultTyping()
    }).print(this).get(inputTimeoutSeconds, TimeUnit.SECONDS)
    Option(value).getOrElse(this).accept2(log)
  }

  def accept2(l: NotebookOutput): Unit
}
