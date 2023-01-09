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

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.dk1844.scala.eventually.FilesHelper.removeFile

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

class EventuallyDataframeTest extends AnyFlatSpec with Matchers with Eventually with BeforeAndAfterAll {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()


  val files = Seq("json1", "json2", "json3", "json4")
  val jsonContent =
    """{"id":1,"key":"A"}
      |{"id":2,"key":"B"}
      |{"id":3,"key":"C"}
      |""".stripMargin

  case class Content(id: Int, key: String)

  override def beforeAll() {
    files.foreach(removeFile)
  }

  override def afterAll() {
    files.foreach(removeFile)
  }

  import FilesHelper._

  "Trivial df read case" should "read the file after it was written (blocking)" in {
    FilesHelper.writeNonEmptyFile("json1", jsonContent)

    val df = spark.read.json("json1")
    df.isEmpty shouldBe false
    df.count() shouldBe 3
  }

  "Trivial df read case" should "fail for file written later (nonblocking)" in {
    writeNonEmptyFileWithDelay("json2", jsonContent)

    val thrown = the[AnalysisException] thrownBy {
      spark.read.json("json2")
    }

    thrown.getMessage() should include("Path does not exist")
  }

  "Eventual df read case" should "succeed in file written later (full block)" in {
    writeNonEmptyFileWithDelay("json3", jsonContent, gradually = true)

    eventually(timeout(scaled(2.seconds)), interval(scaled(50.millis))) {
      val df = spark.read.json("json3")
      df.isEmpty shouldBe false
      df.collectAsList().get(2).get(1) shouldBe "C" // 3rd line, second column
    }

  }

    "Eventual df read case" should "succeed in file written later (partial block)" in {
      writeNonEmptyFileWithDelay("json4", jsonContent, gradually = true)

      val allStates: mutable.Buffer[Boolean] = mutable.Buffer()

      val df = eventually(timeout(scaled(2.seconds)), interval(scaled(50.millis))) {
        val readDf = spark.read.json("json4")
        val isEmpty = readDf.isEmpty
        allStates.append(isEmpty)
        isEmpty shouldBe false

        readDf
      }

      println(s"All states observed: $allStates")

      (0 to 20).foreach { _ =>
        df.isEmpty shouldBe false
        df.collectAsList().get(2).get(1) shouldBe "C" // 3rd line, second column
      }

    }

}
