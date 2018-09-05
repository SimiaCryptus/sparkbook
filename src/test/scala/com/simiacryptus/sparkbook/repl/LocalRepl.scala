package com.simiacryptus.sparkbook.repl

import com.simiacryptus.sparkbook.{LocalRunner, NotebookRunner}

object LocalRepl extends SimpleRepl with LocalRunner with NotebookRunner
