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

import org.apache.spark.graphx.Graph
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{GSSparkSession, SparkSession}

object OrcSSSP extends Logging {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = GSSparkSession.builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    if (args.length < 4) {
      println("Expect 4 args")
      return 0;
    }
    val efile    = args(0)
    val partNum  = args(1).toInt
    val sourceId = args(2).toLong
    val engine   = args(3)
    val time0    = System.nanoTime()
    val loaded   = spark.read.option("inferSchema", true).orc(efile)
    val rdd      = loaded.rdd
    val lineRdd  = rdd.map(row => (row.get(0).asInstanceOf[Long], row.get(1).asInstanceOf[Long])).coalesce(partNum)
    val graph: Graph[Int, Int] = {
      if (engine.equals("gs")) {
        spark.fromLineRDD(lineRdd, partNum)
      } else if (engine.equals("graphx")) {
        Graph.fromEdgeTuples(lineRdd, 1)
      } else {
        throw new IllegalStateException("gs or graphx")
      }
    }
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity).cache()
    log.info(s"initial graph count ${initialGraph.numVertices}, ${initialGraph.numEdges}")
    val time1 = System.nanoTime()

    val sssp = initialGraph
      .pregel(Double.PositiveInfinity)(
        (id, dist, newDist) => {
          math.min(dist, newDist); // Vertex Program
        },
        triplet => { // Send Message
          if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
            Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
          } else {
            Iterator.empty
          }
        },
        (a, b) => math.min(a, b) // Merge Message
      )
      .cache()
    log.info(s"initial graph count ${sssp.numVertices}, ${sssp.numEdges}")
    val time2 = System.nanoTime()
    log.info(s"Pregel took ${(time2 - time1) / 1000000}ms, map load graph ${(time1 - time0) / 1000000}ms")

    spark.close()
  }
}
