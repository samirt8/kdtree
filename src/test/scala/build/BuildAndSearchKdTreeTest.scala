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
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

class BuildAndSearchKdTreeTest extends FunSuite with SharedSparkContext with Serializable {

  // See CheckArgs and CheckArgsTest for an example of how to to execute a main() method
  // and supply the implicit sc:SparkContext parameter.
  test(s"build and search 2-d tree with scala") {

    val args = Array(
      "--input", "src/test/resources/box/inputs/boundingBox.txt",
      "--partitions", "4",
      "--cutoff", "16",
      "--java", "false"
    )

    // Copy the SparkContext so that it is available to SearchKdTree2D.main()
    implicit val sparkContext = sc

    // Search the k-d tree.
    val searchResult: RDD[(Long, List[Long])] = BuildAndSearchKdTree.main(args)

    // Sort the RDD[(Int, List[Int])] by the first Int in case different executions order the elements differently.
    // Convert to an Array, then sort the List[Int] in case different executions order the elements differently.
    val result = searchResult.sortByKey(true).map(e => (e._1, e._2.sorted)).collect
    assert(result(0) == (0,List(3)))
    assert(result(1) == (1,List(2)))
    assert(result(2) == (2,List(1)))
    assert(result(3) == (3,List(0)))
    assert(result(4) == (4,List(11)))
    assert(result(5) == (5,List(13,15)))
    assert(result(6) == (7,List(14)))
    assert(result(7) == (8,List(10)))
    assert(result(8) == (10,List(8)))
    assert(result(9) == (11,List(4)))
    assert(result(10) == (12,List(14)))
    assert(result(11) == (13,List(5,15)))
    assert(result(12) == (14,List(7,12)))
    assert(result(13) == (15,List(5,13)))
  }

  test(s"build and search 2-d tree with java") {

    val args = Array(
      "--input", "src/test/resources/box/inputs/boundingBox.txt",
      "--partitions", "4",
      "--cutoff", "16",
      "--java", "true"
    )

    // Copy the SparkContext so that it is available to SearchKdTree2D.main()
    implicit val sparkContext = sc

    // Search the k-d tree.
    val searchResult: RDD[(Long, List[Long])] = BuildAndSearchKdTree.main(args)

    // Sort the RDD[(Int, List[Int])] by the first Int in case different executions order the elements differently.
    // Convert to an Array, then sort the List[Int] in case different executions order the elements differently.
    val result = searchResult.sortByKey(true).map(e => (e._1, e._2.sorted)).collect
    assert(result(0) == (0,List(3)))
    assert(result(1) == (1,List(2)))
    assert(result(2) == (2,List(1)))
    assert(result(3) == (3,List(0)))
    assert(result(4) == (4,List(11)))
    assert(result(5) == (5,List(13,15)))
    assert(result(6) == (7,List(14)))
    assert(result(7) == (8,List(10)))
    assert(result(8) == (10,List(8)))
    assert(result(9) == (11,List(4)))
    assert(result(10) == (12,List(14)))
    assert(result(11) == (13,List(5,15)))
    assert(result(12) == (14,List(7,12)))
    assert(result(13) == (15,List(5,13)))
  }

  test(s"build and search 4-d tree with scala") {

    val args = Array(
      "--input", "src/test/resources/box/inputs/boundingBox.txt",
      "--partitions", "4",
      "--cutoff", "0",
      "--java", "false"
    )

    // Copy the SparkContext so that it is available to SearchKdTree2D.main()
    implicit val sparkContext = sc

    // Search the k-d tree.
    val searchResult: RDD[(Long, List[Long])] = BuildAndSearchKdTree.main(args)

    // Sort the RDD[(Int, List[Int])] by the first Int in case different executions order the elements differently.
    // Convert to an Array, then sort the List[Int] in case different executions order the elements differently.
    val result = searchResult.sortByKey(true).map(e => (e._1, e._2.sorted)).collect
    assert(result(0) == (0,List(3)))
    assert(result(1) == (1,List(2)))
    assert(result(2) == (2,List(1)))
    assert(result(3) == (3,List(0)))
    assert(result(4) == (4,List(11)))
    assert(result(5) == (5,List(13,15)))
    assert(result(6) == (7,List(14)))
    assert(result(7) == (8,List(10)))
    assert(result(8) == (10,List(8)))
    assert(result(9) == (11,List(4)))
    assert(result(10) == (12,List(14)))
    assert(result(11) == (13,List(5,15)))
    assert(result(12) == (14,List(7,12)))
    assert(result(13) == (15,List(5,13)))
  }

  test(s"build and search 4-d tree with java") {

    val args = Array(
      "--input", "src/test/resources/box/inputs/boundingBox.txt",
      "--partitions", "4",
      "--cutoff", "0",
      "--java", "true"
    )

    // Copy the SparkContext so that it is available to SearchKdTree2D.main()
    implicit val sparkContext = sc

    // Search the k-d tree.
    val searchResult: RDD[(Long, List[Long])] = BuildAndSearchKdTree.main(args)

    // Sort the RDD[(Int, List[Int])] by the first Int in case different executions order the elements differently.
    // Convert to an Array, then sort the List[Int] in case different executions order the elements differently.
    val result = searchResult.sortByKey(true).map(e => (e._1, e._2.sorted)).collect
    assert(result(0) == (0,List(3)))
    assert(result(1) == (1,List(2)))
    assert(result(2) == (2,List(1)))
    assert(result(3) == (3,List(0)))
    assert(result(4) == (4,List(11)))
    assert(result(5) == (5,List(13,15)))
    assert(result(6) == (7,List(14)))
    assert(result(7) == (8,List(10)))
    assert(result(8) == (10,List(8)))
    assert(result(9) == (11,List(4)))
    assert(result(10) == (12,List(14)))
    assert(result(11) == (13,List(5,15)))
    assert(result(12) == (14,List(7,12)))
    assert(result(13) == (15,List(5,13)))
  }

}