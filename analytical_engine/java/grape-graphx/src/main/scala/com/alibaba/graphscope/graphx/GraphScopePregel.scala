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

package com.alibaba.graphscope.graphx

import com.alibaba.graphscope.graphx.utils.SerializationUtils
import com.alibaba.graphscope.utils.MPIUtils
import org.apache.spark.SparkContext
import org.apache.spark.graphx.grape.GrapeGraphImpl
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.GSSparkSession

import scala.reflect.{ClassTag, classTag}

class GraphScopePregel[VD: ClassTag, ED: ClassTag, MSG: ClassTag](
    sc: SparkContext,
    graph: Graph[VD, ED],
    initialMsg: MSG,
    maxIteration: Int,
    activeDirection: EdgeDirection,
    vprog: (VertexId, VD, MSG) => VD,
    sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, MSG)],
    mergeMsg: (MSG, MSG) => MSG
) extends Logging {
  val SERIAL_PATH = "/tmp/graphx-meta"
  val msgClass: Class[MSG] =
    classTag[MSG].runtimeClass.asInstanceOf[java.lang.Class[MSG]]
  val vdClass: Class[VD] =
    classTag[VD].runtimeClass.asInstanceOf[java.lang.Class[VD]]
  val edClass: Class[ED] =
    classTag[ED].runtimeClass.asInstanceOf[java.lang.Class[ED]]

  def run(): Graph[VD, ED] = {
    //Can accept both grapeGraph or GraphXGraph
    val grapeGraph: GrapeGraphImpl[VD, ED] = {
      graph match {
        case graphImpl: GraphImpl[VD, ED] =>
          GSSparkSession.graphXtoGSGraph[VD, ED](graphImpl)
        case grapeGraphImpl: GrapeGraphImpl[VD, ED] => grapeGraphImpl
      }
    }
    //0. write back vertex.
    //1. serialization
    log.info("[Driver:] start serialization functions.")
    val sparkSession = GSSparkSession.getDefaultSession
      .getOrElse(throw new IllegalStateException("empty session"))
      .asInstanceOf[GSSparkSession]
    SerializationUtils.write(
      SERIAL_PATH,
      vdClass,
      edClass,
      msgClass,
      vprog,
      sendMsg,
      mergeMsg,
      initialMsg,
      sc.appName,
      sparkSession.getSocketPath,
      activeDirection
    )

    val numPart = grapeGraph.grapeVertices.getNumPartitions
    //launch mpi processes. and run.
    val t0 = System.nanoTime()

    /** Generate a json string contains necessary info to reconstruct a graphx graph, can be like workerName:
      */
    val fragIds = grapeGraph.fragmentIds.collect()
    log.info(
      s"[GraphScopePregel]: Collected frag ids ${fragIds.mkString(",")}, numPartitions = ${numPart}"
    )

    //running pregel will not change vertex data type.
    MPIUtils.launchGraphX[MSG, VD, ED](
      fragIds,
      vdClass,
      edClass,
      msgClass,
      SERIAL_PATH,
      maxIteration,
      numPart,
      sparkSession.getSocketPath,
      sparkSession.userJarPath
    )
    val newVertexRDD =
      grapeGraph.grapeVertices.updateAfterPIE[ED]().cache() //read from file
    //usually we need to construct graph vertices attributes from vineyard array.

    val t1 = System.nanoTime()
    log.info(
      s"[GraphScopePregel: ] running MPI process cost :${(t1 - t0) / 1000000} ms"
    )
    GrapeGraphImpl.fromExistingRDDs(newVertexRDD, grapeGraph.grapeEdges)
  }
}
