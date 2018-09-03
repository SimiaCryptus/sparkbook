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

import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.sparkbook.Java8Util._
import com.simiacryptus.util.io.{JsonUtil, NotebookOutput}
import com.simiacryptus.util.lang.SerializableConsumer

object EC2Test extends SparkTest with EC2Runner with AWSNotebookRunner {

  override def nodeSettings: EC2NodeSettings = EC2NodeSettings.T2_L

  override def javaOpts = " -Xmx4g -Dspark.master=local:4"

}

object LocalTest extends SimpleTest with LocalRunner with NotebookRunner {

}

object ChildJvmTest extends SimpleTest with ChildJvmRunner with NotebookRunner {

}

class SimpleTest extends SerializableConsumer[NotebookOutput]() {
  override def accept(log: NotebookOutput): Unit = {
    log.eval(()=>{
      "Hello World!"
    })
    log.eval(()=>{
      JsonUtil.toJson(System.getProperties)
    })
    for(i <- 1 to 1000) {
      log.run(()=>{
        Thread.sleep(1000)
      })
    }
  }
}


