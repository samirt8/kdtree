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

package kryo

import context.SharedSparkContext
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.FunSuite

class KryoSerializerTest extends FunSuite with SharedSparkContext with Serializable {

  // Demonstrate that enabling the KryoSerializer throws the following exception for
  // a sorted RDD but not for an unsorted RDD:
  //
  // org.apache.spark.SparkException: Task not serializable
  //
  test("kryo serializer") {

    // Update the SparkContext to specify the KryoSerializer
    val sparkConf: SparkConf = sc.getConf
    sparkConf.set(s"spark.serializer", s"org.apache.spark.serializer.KryoSerializer")
    sparkConf.set(s"spark.kryo.registrationRequired", s"true")
    sparkConf.registerKryoClasses(
      Array(
        classOf[scala.collection.mutable.WrappedArray.ofInt],
        classOf[Array[Int]],
        classOf[Array[Tuple3[_, _, _]]]
      )
    )
    sc.stop
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16), 4)
    val rddPartitionsSizes: Array[Int] = rdd.mapPartitions(iter => Array(iter.size).iterator, true).collect
    rddPartitionsSizes.foreach(ps => println(ps))
    val sortedRdd = rdd.sortBy(e => e, true)
    val sortedRddPartitionsSizes: Array[Int] = sortedRdd.mapPartitions(iter => Array(iter.size).iterator, true).collect
    sortedRddPartitionsSizes.foreach(ps => println(ps))
  }

}