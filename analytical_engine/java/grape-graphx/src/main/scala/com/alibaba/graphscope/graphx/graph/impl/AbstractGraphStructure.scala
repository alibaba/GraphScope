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

package com.alibaba.graphscope.graphx.graph.impl

import com.alibaba.graphscope.ds.{PropertyNbrUnit, Vertex}
import com.alibaba.graphscope.graphx.graph.{GSEdgeTripletImpl, GraphStructure, ReusableEdgeImpl}
import com.alibaba.graphscope.graphx.store.{EdgeDataStore, VertexDataStore}
import com.alibaba.graphscope.graphx.utils.BitSetWithOffset
import com.alibaba.graphscope.utils.FFITypeFactoryhelper
import org.apache.spark.graphx.{Edge, EdgeTriplet}
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

abstract class AbstractGraphStructure extends GraphStructure with Logging {

  def getTripletIterator[VD: ClassTag, ED: ClassTag](
      myNbr: PropertyNbrUnit[Long],
      startLid: Long,
      endLid: Long,
      vertexDataStore: VertexDataStore[VD],
      edatas: EdgeDataStore[ED],
      activeEdgeSet: BitSetWithOffset,
      edgeReversed: Boolean = false,
      includeSrc: Boolean = true,
      includeDst: Boolean = true,
      reuseTriplet: Boolean = false,
      includeLid: Boolean = false
  ): Iterator[EdgeTriplet[VD, ED]] = {
    new Iterator[EdgeTriplet[VD, ED]] {
      var curLid    = startLid.toInt
      val myAddress = myNbr.getAddress
      var curOffset = 0L
      var endOffset = 0L
      val vertex = {
        val r = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
        r.setValue(curLid)
        r
      }
      var srcId   = innerVertexLid2Oid(vertex)
      var srcAttr = vertexDataStore.get(curLid)
      override def hasNext: Boolean = {
        if (curOffset < 0) {
          return false
        }
        if (curOffset < endOffset) {
          true
        } else {
          curLid += 1
          var flag = true
          while (flag && curLid < endLid) {
            curOffset = activeEdgeSet.nextSetBit(getOEBeginOffset(curLid).toInt)
            endOffset = getOEEndOffset(curLid)
            if (curOffset >= endOffset) {
              curLid += 1
            } else flag = false
          }
          if (curLid >= endLid || curOffset < 0) false
          else {
            srcId = innerVertexLid2Oid(vertex)
            srcAttr = vertexDataStore.get(curLid)
            true
          }
        }
      }

      override def next(): EdgeTriplet[VD, ED] = {
        val edgeTriplet = new GSEdgeTripletImpl[VD, ED]
        val curAddress  = curOffset * 16 + myAddress
        myNbr.setAddress(curAddress)
        val dstLid = myNbr.vid().toInt
        val eid    = myNbr.eid().toInt
        vertex.setValue(dstLid)
        if (edgeReversed) {
          edgeTriplet.dstId = srcId
          edgeTriplet.dstAttr = srcAttr
          edgeTriplet.srcId = getId(vertex)
          edgeTriplet.srcAttr = vertexDataStore.get(dstLid)
        } else {
          edgeTriplet.srcId = srcId
          edgeTriplet.srcAttr = srcAttr
          edgeTriplet.dstId = getId(vertex)
          edgeTriplet.dstAttr = vertexDataStore.get(dstLid)
        }
        edgeTriplet.attr = edatas.getWithEID(eid)
        curOffset = activeEdgeSet.nextSetBit(curOffset.toInt + 1)
        edgeTriplet
      }
    }
  }

  def getEdgeIterator[ED: ClassTag](
      myNbr: PropertyNbrUnit[Long],
      startLid: Long,
      endLid: Long,
      edatas: EdgeDataStore[ED],
      activeEdgeSet: BitSetWithOffset,
      edgeReversed: Boolean = false
  ): Iterator[Edge[ED]] = {
    new Iterator[Edge[ED]] {
      var curLid    = startLid.toInt
      val myAddress = myNbr.getAddress
      var curOffset = 0L
      var endOffset = 0L
      val vertex = {
        val t = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
        t.setValue(curLid)
        t
      }
      var srcId = innerVertexLid2Oid(vertex)
      val edge  = new ReusableEdgeImpl[ED]

      override def hasNext: Boolean = {
        if (curOffset < 0) {
          return false
        } else if (curOffset < endOffset) {
          true
        } else {
          curLid += 1
          var flag = true
          while (flag && curLid < endLid) {
            curOffset = activeEdgeSet.nextSetBit(getOEBeginOffset(curLid).toInt)
            endOffset = getOEEndOffset(curLid)
            if (curOffset >= endOffset) {
              curLid += 1
            } else flag = false
          }
          if (curLid >= endLid || curOffset < 0) false
          else {
            vertex.setValue(curLid)
            srcId = innerVertexLid2Oid(vertex)
            true
          }
        }
      }

      override def next(): Edge[ED] = {
        val curAddress = curOffset * 16 + myAddress
        myNbr.setAddress(curAddress)
        val dstLid = myNbr.vid().toInt
        val eid    = myNbr.eid().toInt
        vertex.setValue(dstLid)
        if (edgeReversed) {
          edge.dstId = srcId
          edge.srcId = getId(vertex)
        } else {
          edge.srcId = srcId
          edge.dstId = getId(vertex)
        }
        edge.attr = edatas.getWithEID(eid)
        curOffset = activeEdgeSet.nextSetBit(curOffset.toInt + 1)
        edge
      }
    }
  }
}
