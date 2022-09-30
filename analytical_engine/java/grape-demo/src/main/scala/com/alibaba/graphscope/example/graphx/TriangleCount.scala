/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.example.graphx

import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object TriangleCount extends Logging{
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    if (args.length < 2) {
      println("Expect 1 args")
      return 0;
    }
    val efile = args(0)
    val partNum = args(1).toInt
    val graph: Graph[Int, Int] =
      GraphLoader.edgeListFile(sc, efile,canonicalOrientation = false, partNum)
    // Initialize the graph such that all vertices except the root have distance infinity.
    val triCounts = graph.triangleCount().vertices
    triCounts.saveAsTextFile(s"/tmp/triCounts-${java.time.LocalDateTime.now()}")
    spark.stop()
  }
}
