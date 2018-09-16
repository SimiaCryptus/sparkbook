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
import java.util.concurrent.Future

import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.aws.{EC2Util, Tendril, TendrilControl}
import com.simiacryptus.sparkbook.EC2Runner.{browse, join}
import com.simiacryptus.util.lang.{CodeUtil, SerializableRunnable, SerializableSupplier}
import scala.collection.JavaConversions._

trait BaseRunner[T<:AnyRef] extends SerializableSupplier[T] {
  def nodeSettings: EC2NodeSettings

  def exe(args: String*): T = {
    require(this.isInstanceOf[T])
    main(args.toArray)
    this.asInstanceOf
  }

  def main(args: Array[String]): Unit = {
    val (node, _, _) = start(args)
    browse(node, 1080)
    join(node)
  }

  @transient def runner: EC2RunnerLike

  def cmdFactory(args: Array[String])(node: EC2Util.EC2Node) = this

  def environment: Map[String, String] = Map("SPARK_HOME"->".")

  def start(args: Array[String] = Array.empty): (EC2Util.EC2Node, TendrilControl, Future[T]) = {
    new File("launcher/target/scala-2.11").mkdirs()
    runner.run(nodeSettings, cmdFactory(args), javaopts = javaOpts, _=> new util.HashMap[String, String](environment))
  }

  def maxHeap: Option[String] = Option("16g")

  final def javaProperties: Map[String, String] = Map(
    "spark.master" -> "local[4]"
  )

  final def javaOpts = List(
    maxHeap.map("-Xmx" + _).toList,
    javaProperties.map(e => "-D" + e._1 + "=" + e._2).toList
  ).flatten.mkString(" ")
}
