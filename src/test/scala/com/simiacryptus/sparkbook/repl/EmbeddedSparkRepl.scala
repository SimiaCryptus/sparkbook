package com.simiacryptus.sparkbook.repl

import com.simiacryptus.sparkbook.{EmbeddedSparkRunner, NotebookRunner}

object EmbeddedSparkRepl extends SparkRepl with EmbeddedSparkRunner[Object] with NotebookRunner[Object] {

  override def s3bucket: String = super.s3bucket

  override def numberOfWorkersPerNode: Int = 2

  override def workerMemory: String = "8g"

}
