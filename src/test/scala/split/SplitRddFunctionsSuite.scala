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

package split

import context.SharedSparkContext
import org.scalatest.FunSuite

class SplitRddFunctionsSuite extends FunSuite with SharedSparkContext with Serializable with RddToSplitRddFunctions {

  test(s"takeFirst and takeLast") {
    val rdd = sc.makeRDD(Array((1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60), (7, 70), (8, 80), (9, 90)), 3)
    assert(rdd.takeFirst == (1, 10))
    assert(rdd.takeLast == (9, 90))
    val rdd1 = sc.makeRDD(Array((1, 10)))
    assert(rdd1.takeFirst == (1, 10))
    assert(rdd1.takeLast == (1, 10))
  }

  test(s"splitAt") {
    val rdd = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12), 3)

    var res = rdd.splitAt(0)
    assert(res._1 == None)
    assert(res._2.collect === Array())
    assert(res._3.collect === Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))

    res = rdd.splitAt(1)
    assert(res._1.getOrElse(s"failure to split at 1") == 1)
    assert(res._2.collect === Array())
    assert(res._3.collect === Array(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))

    res = rdd.splitAt(2)
    assert(res._1.getOrElse(s"failure to split at 2") == 2)
    assert(res._2.collect === Array(1))
    assert(res._3.collect === Array(3, 4, 5, 6, 7, 8, 9, 10, 11, 12))

    res = rdd.splitAt(3)
    assert(res._1.getOrElse(s"failure to split at 3") == 3)
    assert(res._2.collect === Array(1, 2))
    assert(res._3.collect === Array(4, 5, 6, 7, 8, 9, 10, 11, 12))

    res = rdd.splitAt(4)
    assert(res._1.getOrElse(s"failure to split at 4") == 4)
    assert(res._2.collect === Array(1, 2, 3))
    assert(res._3.collect === Array(5, 6, 7, 8, 9, 10, 11, 12))

    res = rdd.splitAt(5)
    assert(res._1.getOrElse(s"failure to split at 5") == 5)
    assert(res._2.collect === Array(1, 2, 3, 4))
    assert(res._3.collect === Array(6, 7, 8, 9, 10, 11, 12))

    res = rdd.splitAt(6)
    assert(res._1.getOrElse(s"failure to split at 6") == 6)
    assert(res._2.collect === Array(1, 2, 3, 4, 5))
    assert(res._3.collect === Array(7, 8, 9, 10, 11, 12))

    res = rdd.splitAt(7)
    assert(res._1.getOrElse(s"failure to split at 7") == 7)
    assert(res._2.collect === Array(1, 2, 3, 4, 5, 6))
    assert(res._3.collect === Array(8, 9, 10, 11, 12))

    res = rdd.splitAt(8)
    assert(res._1.getOrElse(s"failure to split at 8") == 8)
    assert(res._2.collect === Array(1, 2, 3, 4, 5, 6, 7))
    assert(res._3.collect === Array(9, 10, 11, 12))

    res = rdd.splitAt(9)
    assert(res._1.getOrElse(s"failure to split at 9") == 9)
    assert(res._2.collect === Array(1, 2, 3, 4, 5, 6, 7, 8))
    assert(res._3.collect === Array(10, 11, 12))

    res = rdd.splitAt(10)
    assert(res._1.getOrElse(s"failure to split at 10") == 10)
    assert(res._2.collect === Array(1, 2, 3, 4, 5, 6, 7, 8, 9))
    assert(res._3.collect === Array(11, 12))

    res = rdd.splitAt(11)
    assert(res._1.getOrElse(s"failure to split at 11") == 11)
    assert(res._2.collect === Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    assert(res._3.collect === Array(12))

    res = rdd.splitAt(12)
    assert(res._1.getOrElse(s"failure to split at 12") == 12)
    assert(res._2.collect === Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
    assert(res._3.collect === Array())

    res = rdd.splitAt(13)
    assert(res._1 == None)
    assert(res._2.collect === Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
    assert(res._3.collect === Array())
  }

}