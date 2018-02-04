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

class BuildKdTreeTest extends FunSuite with SharedSparkContext with Serializable {

  // See CheckArgs and CheckArgsTest for an example of how to to execute a main() method
  // and supply the implicit sc:SparkContext parameter.
  test(s"build 2-d tree via scala") {

    val args = Array(
      "--input", "src/test/resources/box/inputs/boundingBox.txt",
      "--partitions", "8",
      "--cutoff", "16",
      "--java", "false"
    )

    // Copy the SparkContext so that it is available to BuildKdTree2D.main()
    implicit val sparkContext = sc

    val (root, kdNodes, numKdNodes, sortTime, upperTreeTiem, lowerTreeTime, listTime) = BuildKdTree.main(args)

    // Sort the RDD[KdNode] by id in case different executions order the elements differently then convert it to an Array.
    val kdNodesSorted = kdNodes.sortBy(n => n.node.id).collect
    println(kdNodesSorted.length)

    // Check each KdNode.
    assert(kdNodes.count == 16)
    assert(root.toString == s"id:3 xMin:-15 yMin:-31 xMax:27 yMax:18")
    assert(kdNodesSorted(0).toString == s"node:(id:0 xMin:1 yMin:3 xMax:5 yMax:7) lo:(id:14 xMin:-10 yMin:-11 xMax:3 yMax:-7) hi:(id:12 xMin:2 yMin:-12 xMax:7 yMax:-8)")
    assert(kdNodesSorted(1).toString == s"node:(id:1 xMin:15 yMin:12 xMax:20 yMax:17) lo:(None) hi:(None)")
    assert(kdNodesSorted(2).toString == s"node:(id:2 xMin:19 yMin:13 xMax:25 yMax:18) lo:(None) hi:(None)")
    assert(kdNodesSorted(3).toString == s"node:(id:3 xMin:4 yMin:-2 xMax:11 yMax:4) lo:(id:7 xMin:-15 yMin:-25 xMax:7 yMax:7) hi:(id:6 xMin:12 yMin:-31 xMax:27 yMax:18)")
    assert(kdNodesSorted(4).toString == s"node:(id:4 xMin:-9 yMin:-25 xMax:4 yMax:-20) lo:(id:9 xMin:-15 yMin:-18 xMax:-9 yMax:-15) hi:(id:11 xMin:3 yMin:-21 xMax:7 yMax:-18)")
    assert(kdNodesSorted(5).toString == s"node:(id:5 xMin:15 yMin:-31 xMax:18 yMax:-28) lo:(None) hi:(None)")
    assert(kdNodesSorted(6).toString == s"node:(id:6 xMin:15 yMin:-17 xMax:20 yMax:-14) lo:(id:15 xMin:12 yMin:-31 xMax:20 yMax:-25) hi:(id:10 xMin:15 yMin:-5 xMax:27 yMax:18)")
    assert(kdNodesSorted(7).toString == s"node:(id:7 xMin:-15 yMin:-12 xMax:-9 yMax:-8) lo:(id:4 xMin:-15 yMin:-25 xMax:7 yMax:-15) hi:(id:0 xMin:-10 yMin:-12 xMax:7 yMax:7)")
    assert(kdNodesSorted(8).toString == s"node:(id:8 xMin:21 yMin:-2 xMax:27 yMax:2) lo:(None) hi:(id:2 xMin:19 yMin:13 xMax:25 yMax:18)")
    assert(kdNodesSorted(9).toString == s"node:(id:9 xMin:-15 yMin:-18 xMax:-9 yMax:-15) lo:(None) hi:(None)")
    assert(kdNodesSorted(10).toString == s"node:(id:10 xMin:16 yMin:-5 xMax:22 yMax:-1) lo:(id:1 xMin:15 yMin:12 xMax:20 yMax:17) hi:(id:8 xMin:19 yMin:-2 xMax:27 yMax:18)")
    assert(kdNodesSorted(11).toString == s"node:(id:11 xMin:3 yMin:-21 xMax:7 yMax:-18) lo:(None) hi:(None)")
    assert(kdNodesSorted(12).toString == s"node:(id:12 xMin:2 yMin:-12 xMax:7 yMax:-8) lo:(None) hi:(None)")
    assert(kdNodesSorted(13).toString == s"node:(id:13 xMin:12 yMin:-31 xMax:16 yMax:-27) lo:(None) hi:(None)")
    assert(kdNodesSorted(14).toString == s"node:(id:14 xMin:-10 yMin:-11 xMax:3 yMax:-7) lo:(None) hi:(None)")
    assert(kdNodesSorted(15).toString == s"node:(id:15 xMin:14 yMin:-29 xMax:20 yMax:-25) lo:(id:13 xMin:12 yMin:-31 xMax:16 yMax:-27) hi:(id:5 xMin:15 yMin:-31 xMax:18 yMax:-28)")
  }

  test(s"build 2-d tree via java") {

    val args = Array(
      "--input", "src/test/resources/box/inputs/boundingBox.txt",
      "--partitions", "8",
      "--cutoff", "16",
      "--java", "true"
    )

    // Copy the SparkContext so that it is available to BuildKdTree2D.main()
    implicit val sparkContext = sc

    val (root, kdNodes, numKdNodes, sortTime, upperTreeTiem, lowerTreeTime, listTime) = BuildKdTree.main(args)

    // Sort the RDD[KdNode] by id in case different executions order the elements differently then convert it to an Array.
    val kdNodesSorted = kdNodes.sortBy(n => n.node.id).collect
    println(kdNodesSorted.length)

    // Check each KdNode.
    assert(kdNodes.count == 16)
    assert(root.toString == s"id:3 xMin:-15 yMin:-31 xMax:27 yMax:18")
    assert(kdNodesSorted(0).toString == s"node:(id:0 xMin:1 yMin:3 xMax:5 yMax:7) lo:(id:14 xMin:-10 yMin:-11 xMax:3 yMax:-7) hi:(id:12 xMin:2 yMin:-12 xMax:7 yMax:-8)")
    assert(kdNodesSorted(1).toString == s"node:(id:1 xMin:15 yMin:12 xMax:20 yMax:17) lo:(None) hi:(None)")
    assert(kdNodesSorted(2).toString == s"node:(id:2 xMin:19 yMin:13 xMax:25 yMax:18) lo:(None) hi:(None)")
    assert(kdNodesSorted(3).toString == s"node:(id:3 xMin:4 yMin:-2 xMax:11 yMax:4) lo:(id:7 xMin:-15 yMin:-25 xMax:7 yMax:7) hi:(id:6 xMin:12 yMin:-31 xMax:27 yMax:18)")
    assert(kdNodesSorted(4).toString == s"node:(id:4 xMin:-9 yMin:-25 xMax:4 yMax:-20) lo:(id:9 xMin:-15 yMin:-18 xMax:-9 yMax:-15) hi:(id:11 xMin:3 yMin:-21 xMax:7 yMax:-18)")
    assert(kdNodesSorted(5).toString == s"node:(id:5 xMin:15 yMin:-31 xMax:18 yMax:-28) lo:(None) hi:(None)")
    assert(kdNodesSorted(6).toString == s"node:(id:6 xMin:15 yMin:-17 xMax:20 yMax:-14) lo:(id:15 xMin:12 yMin:-31 xMax:20 yMax:-25) hi:(id:10 xMin:15 yMin:-5 xMax:27 yMax:18)")
    assert(kdNodesSorted(7).toString == s"node:(id:7 xMin:-15 yMin:-12 xMax:-9 yMax:-8) lo:(id:4 xMin:-15 yMin:-25 xMax:7 yMax:-15) hi:(id:0 xMin:-10 yMin:-12 xMax:7 yMax:7)")
    assert(kdNodesSorted(8).toString == s"node:(id:8 xMin:21 yMin:-2 xMax:27 yMax:2) lo:(None) hi:(id:2 xMin:19 yMin:13 xMax:25 yMax:18)")
    assert(kdNodesSorted(9).toString == s"node:(id:9 xMin:-15 yMin:-18 xMax:-9 yMax:-15) lo:(None) hi:(None)")
    assert(kdNodesSorted(10).toString == s"node:(id:10 xMin:16 yMin:-5 xMax:22 yMax:-1) lo:(id:1 xMin:15 yMin:12 xMax:20 yMax:17) hi:(id:8 xMin:19 yMin:-2 xMax:27 yMax:18)")
    assert(kdNodesSorted(11).toString == s"node:(id:11 xMin:3 yMin:-21 xMax:7 yMax:-18) lo:(None) hi:(None)")
    assert(kdNodesSorted(12).toString == s"node:(id:12 xMin:2 yMin:-12 xMax:7 yMax:-8) lo:(None) hi:(None)")
    assert(kdNodesSorted(13).toString == s"node:(id:13 xMin:12 yMin:-31 xMax:16 yMax:-27) lo:(None) hi:(None)")
    assert(kdNodesSorted(14).toString == s"node:(id:14 xMin:-10 yMin:-11 xMax:3 yMax:-7) lo:(None) hi:(None)")
    assert(kdNodesSorted(15).toString == s"node:(id:15 xMin:14 yMin:-29 xMax:20 yMax:-25) lo:(id:13 xMin:12 yMin:-31 xMax:16 yMax:-27) hi:(id:5 xMin:15 yMin:-31 xMax:18 yMax:-28)")
  }

  test(s"build 4-d tree via scala") {

    val args = Array(
      "--input", "src/test/resources/box/inputs/boundingBox.txt",
      "--partitions", "4",
      "--cutoff", "0",
      "--java", "false"
    )

    // Copy the SparkContext so that it is available to BuildKdTree2D.main()
    implicit val sparkContext = sc

    val (root, kdNodes, numKdNodes, sortTime, upperTreeTime, lowerTreeTime, listTime) = BuildKdTree.main(args)

    // Sort the RDD[KdNode] by id in case different executions order the elements differently then convert it to an Array.
    val kdNodesSorted = kdNodes.sortBy(n => n.node.id).collect

    // Check each KdNode.
    assert(kdNodes.count == 16)
    assert(root.toString == s"id:3 xMin:-15 yMin:-31 xMax:27 yMax:18")
    assert(kdNodesSorted(0).toString == s"node:(id:0 xMin:1 yMin:3 xMax:5 yMax:7) lo:(id:14 xMin:-10 yMin:-11 xMax:3 yMax:-7) hi:(id:12 xMin:2 yMin:-12 xMax:7 yMax:-8)")
    assert(kdNodesSorted(1).toString == s"node:(id:1 xMin:15 yMin:12 xMax:20 yMax:17) lo:(None) hi:(None)")
    assert(kdNodesSorted(2).toString == s"node:(id:2 xMin:19 yMin:13 xMax:25 yMax:18) lo:(None) hi:(None)")
    assert(kdNodesSorted(3).toString == s"node:(id:3 xMin:4 yMin:-2 xMax:11 yMax:4) lo:(id:7 xMin:-15 yMin:-25 xMax:7 yMax:7) hi:(id:6 xMin:12 yMin:-31 xMax:27 yMax:18)")
    assert(kdNodesSorted(4).toString == s"node:(id:4 xMin:-9 yMin:-25 xMax:4 yMax:-20) lo:(id:9 xMin:-15 yMin:-18 xMax:-9 yMax:-15) hi:(id:11 xMin:3 yMin:-21 xMax:7 yMax:-18)")
    assert(kdNodesSorted(5).toString == s"node:(id:5 xMin:15 yMin:-31 xMax:18 yMax:-28) lo:(id:13 xMin:12 yMin:-31 xMax:16 yMax:-27) hi:(id:15 xMin:14 yMin:-29 xMax:20 yMax:-25)")
    assert(kdNodesSorted(6).toString == s"node:(id:6 xMin:15 yMin:-17 xMax:20 yMax:-14) lo:(id:5 xMin:12 yMin:-31 xMax:20 yMax:-25) hi:(id:10 xMin:15 yMin:-5 xMax:27 yMax:18)")
    assert(kdNodesSorted(7).toString == s"node:(id:7 xMin:-15 yMin:-12 xMax:-9 yMax:-8) lo:(id:4 xMin:-15 yMin:-25 xMax:7 yMax:-15) hi:(id:0 xMin:-10 yMin:-12 xMax:7 yMax:7)")
    assert(kdNodesSorted(8).toString == s"node:(id:8 xMin:21 yMin:-2 xMax:27 yMax:2) lo:(None) hi:(id:2 xMin:19 yMin:13 xMax:25 yMax:18)")
    assert(kdNodesSorted(9).toString == s"node:(id:9 xMin:-15 yMin:-18 xMax:-9 yMax:-15) lo:(None) hi:(None)")
    assert(kdNodesSorted(10).toString == s"node:(id:10 xMin:16 yMin:-5 xMax:22 yMax:-1) lo:(id:1 xMin:15 yMin:12 xMax:20 yMax:17) hi:(id:8 xMin:19 yMin:-2 xMax:27 yMax:18)")
    assert(kdNodesSorted(11).toString == s"node:(id:11 xMin:3 yMin:-21 xMax:7 yMax:-18) lo:(None) hi:(None)")
    assert(kdNodesSorted(12).toString == s"node:(id:12 xMin:2 yMin:-12 xMax:7 yMax:-8) lo:(None) hi:(None)")
    assert(kdNodesSorted(13).toString == s"node:(id:13 xMin:12 yMin:-31 xMax:16 yMax:-27) lo:(None) hi:(None)")
    assert(kdNodesSorted(14).toString == s"node:(id:14 xMin:-10 yMin:-11 xMax:3 yMax:-7) lo:(None) hi:(None)")
    assert(kdNodesSorted(15).toString == s"node:(id:15 xMin:14 yMin:-29 xMax:20 yMax:-25) lo:(None) hi:(None)")
  }

  test(s"build 4-d tree via java") {

    val args = Array(
      "--input", "src/test/resources/box/inputs/boundingBox.txt",
      "--partitions", "4",
      "--cutoff", "0",
      "--java", "true"
    )

    // Copy the SparkContext so that it is available to BuildKdTree2D.main()
    implicit val sparkContext = sc

    val (root, kdNodes, numKdNodes, sortTime, upperTreeTime, lowerTreeTime, listTime) = BuildKdTree.main(args)

    // Sort the RDD[KdNode] by id in case different executions order the elements differently then convert it to an Array.
    val kdNodesSorted = kdNodes.sortBy(n => n.node.id).collect

    // Check each KdNode.
    assert(kdNodes.count == 16)
    assert(root.toString == s"id:3 xMin:-15 yMin:-31 xMax:27 yMax:18")
    assert(kdNodesSorted(0).toString == s"node:(id:0 xMin:1 yMin:3 xMax:5 yMax:7) lo:(id:14 xMin:-10 yMin:-11 xMax:3 yMax:-7) hi:(id:12 xMin:2 yMin:-12 xMax:7 yMax:-8)")
    assert(kdNodesSorted(1).toString == s"node:(id:1 xMin:15 yMin:12 xMax:20 yMax:17) lo:(None) hi:(None)")
    assert(kdNodesSorted(2).toString == s"node:(id:2 xMin:19 yMin:13 xMax:25 yMax:18) lo:(None) hi:(None)")
    assert(kdNodesSorted(3).toString == s"node:(id:3 xMin:4 yMin:-2 xMax:11 yMax:4) lo:(id:7 xMin:-15 yMin:-25 xMax:7 yMax:7) hi:(id:6 xMin:12 yMin:-31 xMax:27 yMax:18)")
    assert(kdNodesSorted(4).toString == s"node:(id:4 xMin:-9 yMin:-25 xMax:4 yMax:-20) lo:(id:9 xMin:-15 yMin:-18 xMax:-9 yMax:-15) hi:(id:11 xMin:3 yMin:-21 xMax:7 yMax:-18)")
    assert(kdNodesSorted(5).toString == s"node:(id:5 xMin:15 yMin:-31 xMax:18 yMax:-28) lo:(id:13 xMin:12 yMin:-31 xMax:16 yMax:-27) hi:(id:15 xMin:14 yMin:-29 xMax:20 yMax:-25)")
    assert(kdNodesSorted(6).toString == s"node:(id:6 xMin:15 yMin:-17 xMax:20 yMax:-14) lo:(id:5 xMin:12 yMin:-31 xMax:20 yMax:-25) hi:(id:10 xMin:15 yMin:-5 xMax:27 yMax:18)")
    assert(kdNodesSorted(7).toString == s"node:(id:7 xMin:-15 yMin:-12 xMax:-9 yMax:-8) lo:(id:4 xMin:-15 yMin:-25 xMax:7 yMax:-15) hi:(id:0 xMin:-10 yMin:-12 xMax:7 yMax:7)")
    assert(kdNodesSorted(8).toString == s"node:(id:8 xMin:21 yMin:-2 xMax:27 yMax:2) lo:(None) hi:(id:2 xMin:19 yMin:13 xMax:25 yMax:18)")
    assert(kdNodesSorted(9).toString == s"node:(id:9 xMin:-15 yMin:-18 xMax:-9 yMax:-15) lo:(None) hi:(None)")
    assert(kdNodesSorted(10).toString == s"node:(id:10 xMin:16 yMin:-5 xMax:22 yMax:-1) lo:(id:1 xMin:15 yMin:12 xMax:20 yMax:17) hi:(id:8 xMin:19 yMin:-2 xMax:27 yMax:18)")
    assert(kdNodesSorted(11).toString == s"node:(id:11 xMin:3 yMin:-21 xMax:7 yMax:-18) lo:(None) hi:(None)")
    assert(kdNodesSorted(12).toString == s"node:(id:12 xMin:2 yMin:-12 xMax:7 yMax:-8) lo:(None) hi:(None)")
    assert(kdNodesSorted(13).toString == s"node:(id:13 xMin:12 yMin:-31 xMax:16 yMax:-27) lo:(None) hi:(None)")
    assert(kdNodesSorted(14).toString == s"node:(id:14 xMin:-10 yMin:-11 xMax:3 yMax:-7) lo:(None) hi:(None)")
    assert(kdNodesSorted(15).toString == s"node:(id:15 xMin:14 yMin:-29 xMax:20 yMax:-25) lo:(None) hi:(None)")
  }

}