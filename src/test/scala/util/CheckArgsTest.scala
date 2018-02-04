/**
 * Copyright (c) 2015, 2016, Russell A. Brown
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors
 *    may be used to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSEARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package util

import context.SharedSparkContext
import org.scalatest.FunSuite

/**
 * It does not appear necessary to provide any information to CheckArgs.main other than 'args'
 * and the implicit SparkContext parameter because all other information is provided by FunSuite
 * and SharedSparkContext when execution occurs as a JUnit test.  For non-JUnit-test-execution,
 * see https://spark.apache.org/docs/1.1.0/submitting-applications.html and in particular
 * the example under "Launching Applications with spark-submit":
 *
 * ./bin/spark-submit \
 * --class <main-class>
 * --master <master-url> \
 * --deploy-mode <deploy-mode> \
 * --conf <key>=<value> \
 * ... # other options
 * <application-jar> \
 * [application-arguments]
 */
class CheckArgsTest extends FunSuite with SharedSparkContext with Serializable {

  test("get args") {

    val args = Array(
      "--input", "partition",
      "--output", "bar",
      "--partitions", "8",
      "--copies", "16",
      "--cutoff", "32",
      "--persist", "MEMORY_AND_DISK_SER",
      "--block", "false",
      "--kryo", "true",
      "--compress", "false",
      "--java", "true"
    )

    // Copy the SparkContext so that it is available to CheckArgs.main()
    implicit val sparkContext = sc

    CheckArgs.main(args)
  }

}