/*
 * Copyright 2023 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.dk1844.scala.eventually

import java.nio.file.{Files, Path, Paths}
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.ExecutionContext.Implicits.global

object FilesHelper {
  def writeNonEmptyFile(filename: String, content: String = "Sample"): Path = {
    Files.write(Paths.get(filename), content.getBytes());
  }

  def writeNonEmptyFileWithDelay(filename: String, content: String = "Sample", delay: FiniteDuration = 500.millis,
    gradually: Boolean = false): Future[Unit] = Future {

    if (gradually) {
      Files.write(Paths.get(filename), Array[Byte]()) // touch first, write later
    }

    Thread.sleep(delay.toMillis)
    writeNonEmptyFile(filename, content)
  }


  def readFileContentSize(filename: String): Int = Files.readAllBytes(Paths.get(filename)).length

  def removeFile(filename: String): Unit = {
    Files.deleteIfExists(Paths.get(filename))
  }


}
