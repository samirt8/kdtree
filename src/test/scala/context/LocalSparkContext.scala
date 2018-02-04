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
 * https://github.com/apache/spark/blob/master/core/src/test/scala/org/apache/spark/LocalSparkContext.scala
 *
 * A slightly different version may be downloaded from the following URL and is commented-out below:
 *
 * https://github.com/erikerlandson/spark/blob/469fbf6758959a793e2b21865ff214c9301b02fd/core/src/test/scala/org/apache/spark/LocalSparkContext.scala
 */
package context

import _root_.io.netty.util.internal.logging.{InternalLoggerFactory, Slf4JLoggerFactory}
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

/** Manages a local `sc` {@link SparkContext} variable, correctly stopping it after each test. */
trait LocalSparkContext extends BeforeAndAfterEach with BeforeAndAfterAll { self: Suite =>

  @transient var sc: SparkContext = _

  override def beforeAll() {
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory())
    super.beforeAll()
  }

  override def afterEach() {
    resetSparkContext()
    super.afterEach()
  }

  def resetSparkContext(): Unit = {
    LocalSparkContext.stop(sc)
    sc = null
  }

}

object LocalSparkContext {
  def stop(sc: SparkContext) {
    if (sc != null) {
      sc.stop()
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  /** Runs `f` by passing in `sc` and ensures that `sc` is stopped. */
  def withSpark[T](sc: SparkContext)(f: SparkContext => T): T = {
    try {
      f(sc)
    } finally {
      stop(sc)
    }
  }

//import org.apache.spark.SparkContext
//import org.jboss.netty.logging.{InternalLoggerFactory, Slf4JLoggerFactory}
//import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
//
///** Manages a local `sc` {@link SparkContext} variable, correctly stopping it after each test. */
//trait LocalSparkContext extends BeforeAndAfterEach with BeforeAndAfterAll { self: Suite =>
//
//  @transient var sc: SparkContext = _
//
//  override def beforeAll() {
//    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory())
//    super.beforeAll()
//  }
//
//  override def afterEach() {
//    resetSparkContext()
//    super.afterEach()
//  }
//
//  def resetSparkContext() = {
//    LocalSparkContext.stop(sc)
//    sc = null
//  }
//
//}
//
//object LocalSparkContext {
//  def stop(sc: SparkContext) {
//    if (sc != null) {
//      sc.stop()
//    }
//    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
//    System.clearProperty("spark.driver.port")
//  }
//
//  /** Runs `f` by passing in `sc` and ensures that `sc` is stopped. */
//  def withSpark[T](sc: SparkContext)(f: SparkContext => T) = {
//    try {
//      f(sc)
//    } finally {
//      stop(sc)
//    }
//  }

}
