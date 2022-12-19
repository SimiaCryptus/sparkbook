/*
 * Copyright (c) 2019 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.simiacryptus.sparkbook.repl

import java.util.concurrent.TimeUnit

import com.simiacryptus.notebook.StringQuery.SimpleStringQuery
import com.simiacryptus.notebook.{MarkdownNotebookOutput, NotebookOutput}
import com.simiacryptus.sparkbook._
import com.simiacryptus.sparkbook.util.Java8Util._
import javax.script.ScriptEngineManager

import scala.reflect.runtime.currentMirror
import scala.tools.nsc.interpreter.IMain
import scala.tools.reflect.ToolBox

object SimpleScalaRepl {

  @transient private lazy val engine = new ScriptEngineManager().getEngineByName("scala")
  @transient private lazy val toolbox = currentMirror.mkToolBox()

  def eval_repl(code: String) = {
    engine.asInstanceOf[IMain].settings.embeddedDefaults[SimpleScalaRepl[_]]
    engine.eval(code)
  }

  def eval_toolkit(code: String) = {
    toolbox.eval(toolbox.parse(code)).asInstanceOf[Object]
  }
}

import com.simiacryptus.sparkbook.repl.SimpleScalaRepl._

class SimpleScalaRepl[T<:SimpleScalaRepl[T]] extends InteractiveSetup[Object, T] {
  override val inputTimeoutSeconds = 300
  val defaultCode = """throw new RuntimeException("End Application")"""

  def postConfigure(log: NotebookOutput): Object = {
    while (true) {
      def code = {
        new SimpleStringQuery(log.asInstanceOf[MarkdownNotebookOutput]).setValue(defaultCode).print().get(inputTimeoutSeconds, TimeUnit.SECONDS)
      }

      log.eval(() => {
        eval_toolkit(code)
      })
    }
    null
  }

  override def s3bucket: String = ""
}


