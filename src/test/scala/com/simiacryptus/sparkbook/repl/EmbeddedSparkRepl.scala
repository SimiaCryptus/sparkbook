package com.simiacryptus.sparkbook.repl

import com.simiacryptus.sparkbook.{EmbeddedSparkRunner, NotebookRunner}

object EmbeddedSparkRepl extends SparkRepl with EmbeddedSparkRunner with NotebookRunner {

  override def numberOfWorkersPerNode: Int = 2

  override def workerMemory: String = "8g"

}
