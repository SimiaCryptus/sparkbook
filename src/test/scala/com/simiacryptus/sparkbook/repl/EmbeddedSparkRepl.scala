package com.simiacryptus.sparkbook.repl

import com.simiacryptus.sparkbook.{EmbeddedSparkRunner, NotebookRunner}

object EmbeddedSparkRepl extends SparkRepl with EmbeddedSparkRunner[Object] with NotebookRunner[Object] {

  override protected val s3bucket: String = envTuple._2

  override def numberOfWorkersPerNode: Int = 2

  override def workerMemory: String = "8g"

}
