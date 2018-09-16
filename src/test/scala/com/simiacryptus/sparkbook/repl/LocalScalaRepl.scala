package com.simiacryptus.sparkbook.repl

import com.simiacryptus.sparkbook.{LocalRunner, NotebookRunner}

object LocalScalaRepl extends SimpleScalaRepl with LocalRunner[Object] with NotebookRunner[Object]
