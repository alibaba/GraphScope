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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{GSSparkSession, SparkSession}

object GSSessionTest extends Logging{
  def main(args: Array[String]) : Unit = {
    val session = GSSparkSession.builder().vineyardMemory("10Gi").getOrCreate()

    if (args.length != 2){
      log.error(s"Expect 2 params, receive ${args.length}")
      return
    }
    val vFile = args(0)
    val eFile = args(1)
    val graph = session.loadGraphToGS[Long,Long](vFile,eFile,64)
    val res = graph.mapVertices((vid,vd) => vd).mapTriplets(triplet => triplet.srcId).cache()

    log.info(s"load graph to gs numVertices ${res.numVertices}, edges ${res.numEdges}")
    session.close()
  }
}
