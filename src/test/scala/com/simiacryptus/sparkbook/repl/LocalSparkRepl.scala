package com.simiacryptus.sparkbook.repl

import com.simiacryptus.sparkbook.NotebookRunner
import com.simiacryptus.sparkbook.util.LocalRunner

object LocalSparkRepl extends SparkRepl with LocalRunner[Object] with NotebookRunner[Object]
