package com.simiacryptus.sparkbook.repl

import com.simiacryptus.sparkbook.{ChildJvmRunner, NotebookRunner}

object ChildJvmScalaRepl extends SimpleScalaRepl with ChildJvmRunner with NotebookRunner
