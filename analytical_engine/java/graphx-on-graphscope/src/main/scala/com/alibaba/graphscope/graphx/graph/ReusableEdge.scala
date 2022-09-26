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

package com.alibaba.graphscope.graphx.graph

import org.apache.spark.graphx.{Edge, VertexId}

trait ReusableEdge[ED] extends Edge[ED]{
  var eid : Long = -1
  var offset : Long = -1
//  var eid : Long = -1
  def setSrcId(vertexId: VertexId)
  def setDstId(vertexId: VertexId)
  def setAttr(ed : ED)
}

class ReusableEdgeImpl[@specialized(Long,Int,Double)ED] extends ReusableEdge[ED] {
  override def setSrcId(vertexId: VertexId) = {
    this.srcId = vertexId
  }
  override def setDstId(vertexId: VertexId) = {
    this.dstId = vertexId
  }

  override def setAttr(ed : ED) = {
    this.attr = ed
  }

  override def toString: String = {
    "GSEdge(src=" + srcId + ",dst=" + dstId + ",attr=" + attr +")"
  }
}
