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

package com.simiacryptus.sparkbook

import com.google.gson.{GsonBuilder, JsonObject, JsonPrimitive}
import com.simiacryptus.notebook.Jsonable
import com.simiacryptus.sparkbook.util.LocalRunner


trait LocalAWSNotebookRunner[R,V<:LocalAWSNotebookRunner[R,V]] extends LocalRunner[R] with NotebookRunner[R] with Jsonable[V] {

  var emailAddress : String = ""
  def s3bucket : String

  override def toJson: String = {
    val json = super.toJson
    val gson = new GsonBuilder().setPrettyPrinting().create()
    val jsonObject = gson.fromJson(json, classOf[JsonObject])
    jsonObject.add("emailAddress", new JsonPrimitive(emailAddress))
    jsonObject.add("s3bucket", new JsonPrimitive(s3bucket.toString))
    gson.toJson(jsonObject)
  }

}


