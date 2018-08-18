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
package com.simiacryptus.sparkbook

import java.io.File
import java.util.function.Consumer

import com.simiacryptus.aws.Tendril
import com.simiacryptus.aws.Tendril.SerializableConsumer
import com.simiacryptus.util.Util
import com.simiacryptus.util.io.{MarkdownNotebookOutput, NotebookOutput}
import com.simiacryptus.util.test.SysOutInterceptor

/**
  * The type Local runner.
  */
object LocalRunner {
    @throws[InstantiationException]
    @throws[IllegalAccessException]
    @throws[ClassNotFoundException]
    def getTask[T <: SerializableConsumer[_]](defaultClass: Class[T], args: String*): SerializableConsumer[NotebookOutput] = (if (args.length == 0) defaultClass
    else Class.forName(args(0))).newInstance.asInstanceOf[SerializableConsumer[NotebookOutput]]

  /**
    * Run.
    *
    * @param fns the fns
    * @throws Exception the exception
    */
  @throws[Exception]
  def run(fns: Consumer[NotebookOutput]*): Unit = for (fn <- fns) {
    try {
      val log = new MarkdownNotebookOutput(new File("report/" + Util.dateStr("yyyyMMddHHmmss") + "/index"), "", Util.AUTO_BROWSE)
      try {
        fn.accept(log)
        log.setFrontMatterProperty("status", "OK")
      } finally {
        System.exit(0)
        if (log != null) log.close()
      }
    }
  }

  try SysOutInterceptor.INSTANCE.init
}

abstract class LocalRunner(klass : Class[_<:SerializableConsumer[_]]) {
  def main(args:Array[String]):Unit = {
    System.setProperty("spark.master","local[4]")
    System.setProperty("spark.app.name","local")
    LocalRunner.run( LocalRunner.getTask (klass) )
  }
}