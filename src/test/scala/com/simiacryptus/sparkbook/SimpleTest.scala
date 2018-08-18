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

import com.simiacryptus.aws.Tendril
import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.util.io.{JsonUtil, NotebookOutput}

object EC2Test extends EC2NotebookRunner(EC2NodeSettings.StandardJavaAMI, classOf[SparkTest]) {
  override def JAVA_OPTS = " -Xmx4g -Dspark.master=local:4"
}

object LocalTest extends LocalRunner(classOf[SimpleTest]) {

}

class SimpleTest extends Tendril.SerializableConsumer[NotebookOutput]() {
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


