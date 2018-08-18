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

import com.simiacryptus.util.io.{JsonUtil, ScalaJson}
import java.io.File
import java.io.IOException
import java.util.Scanner

import Java8Util._

object UserSettings {
    lazy val load: UserSettings = try ScalaJson.cache(new File("user-settings.json"), classOf[UserSettings], () => {
      val userSettings = new UserSettings
      val scanner = new Scanner(System.in)
      System.out.print("Enter user email address: ")
      userSettings.copy(emailAddress = scanner.nextLine)
    })
    catch {
      case e: IOException =>
        throw new RuntimeException(e)
    }
}

case class UserSettings(emailAddress: String = null)