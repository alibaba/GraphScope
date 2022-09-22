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

object StringVEDTest extends Logging{
  def main(args: Array[String]) : Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    if (args.length < 2) {
      println("Expect 1 args")
      return 0;
    }
    val eFilePath = args(0);
    val numPartitions = args(1).toInt;

    val graph : Graph[Long,Long] = GraphLoader.edgeListFile(sc, eFilePath, false, numEdgePartitions = numPartitions).mapVertices((id,vd)=>vd.toLong).mapEdges(edge=>edge.attr.toLong).cache()
    val graph2 = graph.mapVertices((id,vd) => id).mapVertices((id,vd)=>(vd,vd))
    val graph3 = graph2.mapEdges(edge => (edge.srcId,edge.dstId))
    val res = graph3.pregel((5L,5L), maxIterations = 10)(
      (id, dist, newDist) => {
        log.info(s"visiting vertex ${id}(${dist}), new dist${newDist}")
        newDist
      },
      triplet => { // Send Message
        log.info(s"visiting triplet ${triplet.srcId}(${triplet.srcAttr}) -> ${triplet.dstId}(${triplet.dstAttr}), attr ${triplet.attr}")
        Iterator((triplet.dstId, (2L,2L)))
      },
      (a, b) => (Math.min(a._1,b._1),Math.max(a._2,b._2)) // Merge Message
    )
    sc.stop()
  }
}
