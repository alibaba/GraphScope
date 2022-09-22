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

import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{GSSparkSession, SparkSession}

object OperatorTest extends Logging{
    def main(array: Array[String]) : Unit = {
      require(array.length == 2)
      val fileName = array(0)
      val partNum = array(1).toInt
      val spark = SparkSession
        .builder
        .appName(s"${this.getClass.getSimpleName}")
        .getOrCreate()
      val sc = spark.sparkContext

      val rawGraph = GraphLoader.edgeListFile(sc, fileName,false, partNum)
      val graph = rawGraph.mapVertices((vid,vd)=>vd.toLong).mapEdges(edge=>edge.attr.toLong).cache()
      val grapeGraph = GSSparkSession.graphXtoGSGraph[Long,Long](graph.asInstanceOf[GraphImpl[Long,Long]]).cache()


      def mapping(graph : Graph[Long,Long])  : Graph[Long,Long] = {
        log.info("[Operator test]: start Mapping")
        graph.mapVertices((vid, vd) => vd + vid)
          .mapEdges(edge=> edge.srcId + edge.dstId + edge.attr)
          .mapTriplets(triplet => triplet.srcAttr + triplet.dstAttr + triplet.attr + triplet.srcId + triplet.dstId)
      }

      def outerJoin(graph : Graph[Long,Long]) : Graph[Long,Long] = {
        log.info("[Operator test]: start outer join")
        val inDegrees = graph.inDegrees
        graph.joinVertices[Int](inDegrees)((id, ovd, newVd) => {
          newVd.toLong
        })
      }

      val graphxRes = outerJoin(mapping(graph))
      val grapeRes = outerJoin(mapping(grapeGraph))

      log.info(s"after test grape rdd ${grapeRes.vertices.count()}, edges ${grapeRes.edges.count()}")
      log.info(s"after test graphx rdd ${graphxRes.vertices.count()}, edges ${graphxRes.edges.count()}")

      sc.stop()
    }
  }

