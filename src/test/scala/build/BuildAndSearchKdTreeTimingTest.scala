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

package build

import context.SharedSparkContext
import org.scalatest.FunSuite

class BuildAndSearchKdTreeTimingTest extends FunSuite with SharedSparkContext with Serializable {

  // See CheckArgs and CheckArgsTest for an example of how to to execute a main() method
  // and supply the implicit sc:SparkContext parameter.
  test(s"build and search k-d tree timing") {

    // All required commmand-line arguments are supplied by default in the BuildAndSearchKdTreeTiming.main()
    // method but the following combination of arguments tests RDD- and array-based tree building.
    val args = Array("--copies", "16", "--partitions", "16", "--cutoff", "512")

    // Copy the SparkContext so that it is available to BuildAndSearchKdTree2D.main()
    implicit val sparkContext = sc

    // Create bounding boxes, build then search a k-d tree and report the elapsed times in milliseconds.
    BuildAndSearchKdTreeTiming.elapsedTimes(args)
  }

}