/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This code was downloaded from the apache spark distribution at the following URL then modified slightly:
 *
 * https://github.com/apache/spark/blob/master/core/src/test/scala/org/apache/spark/SharedSparkContext.scala
 *
 * A slightly different version may be downloaded from the following URL and is commented out below:
 *
 * https://github.com/erikerlandson/spark/blob/469fbf6758959a793e2b21865ff214c9301b02fd/core/src/test/scala/org/apache/spark/SharedSparkContext.scala
 */
package context

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Shares a local `SparkContext` between all tests in a suite and closes it at the end */
trait SharedSparkContext extends BeforeAndAfterAll { self: Suite =>

  @transient private var _sc: SparkContext = _

  def sc: SparkContext = _sc

  var conf = new SparkConf(false)

  override def beforeAll() {
    _sc = new SparkContext("local[4]", "test", conf)
    super.beforeAll()
  }

  override def afterAll() {
    LocalSparkContext.stop(_sc)
    _sc = null
    super.afterAll()
  }

  //trait SharedSparkContext extends BeforeAndAfterAll { self: Suite =>
  //
  //  @transient private var _sc: SparkContext = _
  //
  //  def sc: SparkContext = _sc
  //
  //  var conf = new SparkConf(false)
  //
  //  override def beforeAll() {
  //    _sc = new SparkContext("local", "test", conf)
  //    super.beforeAll()
  //  }
  //
  //  override def afterAll() {
  //    LocalSparkContext.stop(_sc)
  //    _sc = null
  //    super.afterAll()
  //  }
}
