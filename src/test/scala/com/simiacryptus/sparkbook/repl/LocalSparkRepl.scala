package com.simiacryptus.sparkbook.repl

import com.simiacryptus.sparkbook.{LocalRunner, NotebookRunner}

object LocalSparkRepl extends SparkRepl with LocalRunner with NotebookRunner
