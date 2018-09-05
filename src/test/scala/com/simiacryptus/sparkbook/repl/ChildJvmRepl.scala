package com.simiacryptus.sparkbook.repl

import com.simiacryptus.sparkbook.{ChildJvmRunner, NotebookRunner}

object ChildJvmRepl extends SimpleRepl with ChildJvmRunner with NotebookRunner
