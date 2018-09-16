/*
 * Copyright (c) 2018 by Andrew Charneski.
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

import com.simiacryptus.sparkbook.Java8Util._
import com.simiacryptus.sparkbook._
import com.simiacryptus.util.io.StringQuery.SimpleStringQuery
import com.simiacryptus.util.io._
import javax.script.ScriptEngineManager
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox
import scala.tools.nsc.interpreter.IMain

object SimpleScalaRepl {

  @transient private lazy val engine = new ScriptEngineManager().getEngineByName("scala")

  def eval_repl(code: String) = {
    engine.asInstanceOf[IMain].settings.embeddedDefaults[SimpleScalaRepl]
    engine.eval(code)
  }

  @transient private lazy val toolbox = currentMirror.mkToolBox()

  def eval_toolkit(code: String) = {
    toolbox.eval(toolbox.parse(code)).asInstanceOf[Object]
  }
}

import com.simiacryptus.sparkbook.repl.SimpleScalaRepl._

class SimpleScalaRepl extends InteractiveSetup[Object] {
  val defaultCode = """throw new RuntimeException("End Application")"""
  override val inputTimeoutSeconds = 300

  def accept2(log: NotebookOutput): Object = {
    while (true) {
      def code = {
        new SimpleStringQuery(log.asInstanceOf[MarkdownNotebookOutput]).print(defaultCode).get(inputTimeoutSeconds, TimeUnit.SECONDS)
      }

      log.eval(() => {
        eval_toolkit(code)
      })
    }
    null
  }

}


