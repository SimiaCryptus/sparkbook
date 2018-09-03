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
import java.util

import com.simiacryptus.aws.Tendril
import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.util.lang.SerializableRunnable
import Java8Util._
import scala.collection.JavaConversions._

import scala.util.Random

trait ChildJvmRunner extends BaseRunner with Logging {
  def workingDir = new File(".")

  def environment: Map[String, String] = Map()

  override lazy val runner: EC2RunnerLike = new LocalBaseRunner with Logging {
    lazy val control = Tendril.startLocalJvm(18000 + Random.nextInt(1024), javaOpts, new util.HashMap[String, String](environment), workingDir)

    override def run(task: SerializableRunnable) = {
      control.start(() => task.run())
    }

    override def status: String = {
      try {
        control.eval(() => "running")
      } catch {
        case e: Throwable =>
          logger.warn("Error getting child status", e)
          e.getMessage
      }
    }
  }

  override def nodeSettings: EC2NodeSettings = null
}