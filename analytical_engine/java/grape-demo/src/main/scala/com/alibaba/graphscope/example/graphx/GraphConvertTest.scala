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

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{GSSparkSession, SparkSession}

object GraphConvertTest extends Logging {
  def main(array: Array[String]): Unit = {
    require(array.length == 2)
    val path    = array(0)
    val numPart = array(1).toInt
    val spark = GSSparkSession.builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    val graphxGraph = GraphLoader
      .edgeListFile(sc, path, false, numPart)
      .mapVertices((id, vd) => id)
      .mapEdges(edge => edge.dstId)
      .cache()
    log.info(s"Loaded graphx graph ${graphxGraph.numVertices} vertices, ${graphxGraph.numEdges} edges")

    val grapeGraph = GSSparkSession.graphXtoGSGraph(graphxGraph.asInstanceOf[GraphImpl[Long, Long]])
    log.info(s"Converted to grape graph ${grapeGraph.numVertices} vertices, ${grapeGraph.numEdges} edges")
    val res = grapeGraph.mapVertices((id, vd) => vd + 10).mapTriplets(triplet => triplet.attr + 1).cache()
    log.info(s"Converted to grape graph ${res.numVertices} vertices, ${res.numEdges} edges")

    spark.close()
  }
}
