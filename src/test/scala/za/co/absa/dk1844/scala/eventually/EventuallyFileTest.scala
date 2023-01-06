/*
 * Copyright 2018 ABSA Group Limited
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

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.dk1844.scala.eventually.FilesHelper.removeFile

import java.nio.file.{Files, NoSuchFileException, Path, Paths}
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.ExecutionContext.Implicits.global

object FilesHelper {
  def writeNonEmptyFile(filename: String): Path = {
    Files.write(Paths.get(filename), "ContentX".getBytes());
  }

  def writeNonEmptyFileWithDelay(filename: String, delay: FiniteDuration = 500.millis): Future[Unit] = Future {
    Thread.sleep(delay.toMillis)
    writeNonEmptyFile(filename)
  }

  def readFileContentSize(filename: String): Int = Files.readAllBytes(Paths.get(filename)).length

  def removeFile(filename: String): Unit = {
    Files.deleteIfExists(Paths.get(filename))
  }


}

class EventuallyFullEnclosedTest extends AnyFlatSpec with Matchers with Eventually with BeforeAndAfterAll {

  val files = Seq("file1", "file2", "file3", "file4")

  override def beforeAll() {
    files.foreach(removeFile)
  }

  override def afterAll() {
    files.foreach(removeFile)
  }

  import FilesHelper._

  "Trivial read case " should "read the file after it was written (blocking)" in {
    writeNonEmptyFile("file1")
    val size = readFileContentSize("file1")
    size should be > 0
  }

  "Trivial read case" should "fail for file written later (nonblocking)" in {
    writeNonEmptyFileWithDelay("file2")

    an[NoSuchFileException] should be thrownBy {
      readFileContentSize("file2")
    }
  }

  "Eventual read case" should "succeed in file written later (full block)" in {
    writeNonEmptyFileWithDelay("file3")

    eventually(timeout(scaled(2.seconds)), interval(scaled(50.millis))) {
      val size = readFileContentSize("file3")
      size should be > 0
    }

  }

  "Eventual read case" should "succeed in file written later (partial block)" in {
    writeNonEmptyFileWithDelay("file4")

    val allStates: mutable.Buffer[Int] = mutable.Buffer()

    val size: Int = eventually(timeout(scaled(2.seconds)), interval(scaled(50.millis))) {
      val contentSize = readFileContentSize("file4")
      allStates.append(contentSize)
      contentSize
    }

    // returned value
    size should be > 0
    println(s"All states observed: $allStates")
  }

}
