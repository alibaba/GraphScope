/*
 * The file GiraphConfiguration.java is referred and derived from
 * project apache/spark,
 *
 *    https://github.com/apache/spark.git
 *
 * which has the following license:
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.graphx.lib

import org.apache.spark.graphx._
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

object TriangleCount extends Logging with Serializable {
  var stage = 0

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Int, ED] = {

    val tmp = graph.outerJoinVertices(
      graph.collectNeighborIds(edgeDirection = EdgeDirection.Either)
    )((vid, vd, nbrIds) => nbrIds.get)

    val triangleGraph = tmp.mapVertices((vid, vd) => (0, vd.toSet))
    stage = 0

    def vp(
        id: VertexId,
        attr: (Int, Set[VertexId]),
        msg: Array[VertexId]
    ): (Int, Set[VertexId]) = {
      if (stage == 0) {
        attr
      } else if (stage == 1) {
        val prevCnt = attr._1
        var curCnt = 0
        var i = 0
        val size = msg.size
        while (i < size) {
          if (attr._2.contains(msg(i))) {
            curCnt += 1
          }
          i += 1
        }
        (prevCnt + curCnt, attr._2)
      } else {
        attr
      }
    }

    def sendMsg(
        edge: EdgeTriplet[(Int, Set[VertexId]), ED]
    ): Iterator[(VertexId, Array[VertexId])] = {
      if (stage == 0) {
        stage = 1
        log.info(
          s"${edge.srcId} send msg to ${edge.dstId}, ${edge.srcAttr._2.toArray.mkString(",")}"
        )
        Iterator((edge.dstId, edge.srcAttr._2.toArray))
      } else {
        Iterator.empty
      }
    }

    def mergeMsg(a: Array[VertexId], b: Array[VertexId]): Array[VertexId] = {
      val res = a ++ b
      res
    }

    val initialMsg = new Array[VertexId](1)
    triangleGraph
      .pregel(initialMsg)(vp, sendMsg, mergeMsg)
      .mapVertices((vid, vd) => vd._1)
  }
}
