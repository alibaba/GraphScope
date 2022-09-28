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

object PageRankTest extends Logging{
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = GSSparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    if (args.length < 3) {
      println("Expect 4 args")
      return 0;
    }
    val efile = args(0)
    val partNum = args(1).toInt
    val engine = args(2)
    val time0 = System.nanoTime()

    val orcRDD = spark.read.option("inferSchema",true).orc(efile).rdd.map(row => (row.get(0).asInstanceOf[Long], row.get(1).asInstanceOf[Long]))

    val graph: Graph[Int, Double] = {
      if (engine.equals("gs")) {
        spark.fromLineRDD(orcRDD, partNum).mapEdges(e => e.attr.toDouble).cache()
      }
      else if (engine.equals("graphx")){
        //graphx has no variant of edges.
        val orcRDD2 = orcRDD.coalesce(partNum)
        Graph.fromEdgeTuples(orcRDD2,1).mapEdges(e => e.attr.toDouble).cache()
      }
      else {
        throw new IllegalStateException("gs or graphx")
      }
    }
    // Initialize the graph such that all vertices except the root have distance infinity.
    log.info(s"initial graph count ${graph.numVertices}, ${graph.numEdges}")
    val time1 = System.nanoTime()

    val pagerank = graph.pageRank(0.001).cache()

    log.info(s"pagerank graph count ${graph.numVertices}, ${graph.numEdges}")
    val time2 = System.nanoTime()
    log.info(s"Pregel took ${(time2 - time1)/1000000}ms, load graph ${(time1 - time0)/1000000}ms")

    spark.stop()

  }
}
