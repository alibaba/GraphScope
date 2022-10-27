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

import com.alibaba.graphscope.ds.Vertex
import com.alibaba.graphscope.graphx.graph.GraphStructureTypes.GraphStructureType
import com.alibaba.graphscope.graphx.store.{EdgeDataStore, VertexDataStore}
import com.alibaba.graphscope.graphx.utils.BitSetWithOffset
import com.alibaba.graphscope.utils.ThreadSafeBitSet
import org.apache.spark.graphx.{Edge, EdgeTriplet}

import scala.reflect.ClassTag

/**
 * Defines the interface of graph structure, include vm, csr. But doesn't contain vertex attribute
 * and edge attribute(or contained but we don't use)
 */
object GraphStructureTypes extends Enumeration{
  type GraphStructureType = Value
  val GraphXFragmentStructure,ArrowProjectedStructure = Value
}

trait GraphStructure extends Serializable {

  val structureType : GraphStructureType

  def inDegreeArray(startLid : Long, endLid : Long) :Array[Int]

  def outDegreeArray(startLid : Long, endLid : Long) : Array[Int]

  def inOutDegreeArray(startLid : Long, endLid : Long) : Array[Int]

  def iterator[ED : ClassTag](startLid : Long, endLid : Long, edatas : EdgeDataStore[ED], activeSet: BitSetWithOffset, reversed : Boolean = false) : Iterator[Edge[ED]]

  def tripletIterator[VD: ClassTag,ED : ClassTag](startLid : Long, endLid : Long, vertexDataStore: VertexDataStore[VD], edatas : EdgeDataStore[ED], activeSet: BitSetWithOffset, edgeReversed : Boolean = false, includeSrc: Boolean = true, includeDst: Boolean = true, reuseTriplet : Boolean = false, includeLid : Boolean = false): Iterator[EdgeTriplet[VD, ED]]

  def iterateEdges[ED : ClassTag,ED2 : ClassTag](startLid : Long, endLid : Long, f: Edge[ED] => ED2, edatas : EdgeDataStore[ED], activeSet : BitSetWithOffset, edgeReversed : Boolean = false, newArray : EdgeDataStore[ED2]) : Unit

  def iterateTriplets[VD : ClassTag, ED : ClassTag,ED2 : ClassTag](startLid : Long, endLid : Long, f : EdgeTriplet[VD,ED] => ED2, activeVertices : BitSetWithOffset, innerVertexDataStore: VertexDataStore[VD], edatas : EdgeDataStore[ED], activeSet : BitSetWithOffset, edgeReversed : Boolean = false, includeSrc : Boolean = true, includeDst : Boolean = true, newArray : EdgeDataStore[ED2]) : Unit

  /** get the oe begin offset */
  def getOEBeginOffset(vid: Int) : Long

  def getOEEndOffset(vid: Int) : Long

  def getInDegree(vertex: Vertex[Long]): Long

  def getOutDegree(vertex: Vertex[Long]): Long

  /** get the ie begin offset */
  def getIEBeginOffset(vid: Int) : Long

  def getIEEndOffset(vid: Int) : Long

  def getOutNbrIds(vertex: Vertex[Long]) : Array[Long]

  def getInNbrIds(vertex: Vertex[Long]) : Array[Long]

  def getInOutNbrIds(vertex: Vertex[Long]) : Array[Long]

  def isInEdgesEmpty(vertex: Vertex[Long]): Boolean

  def isOutEdgesEmpty(vertex: Vertex[Long]): Boolean

  def getInEdgesNum: Long

  def getOutEdgesNum: Long

  //all edges in this frag
  def getTotalEdgesNum : Long

  def fid(): Int

  def fnum(): Int

  def getId(vertex: Vertex[Long]): Long

  def getVertex(oid: Long, vertex: Vertex[Long]): Boolean

  def getOuterVertex(oid : Long, vertex : Vertex[Long]) : Boolean

  def getInnerVertex(oid : Long, vertex: Vertex[Long]) : Boolean

  def getTotalVertexSize: Long

  def getVertexSize: Long

  def getInnerVertexSize: Long

  def innerVertexLid2Oid(vertex: Vertex[Long]): Long

  def outerVertexLid2Oid(vertex: Vertex[Long]): Long

  def getOuterVertexSize: Long

  def getOuterVertexGid(vertex: Vertex[Long]): Long

  def fid2GraphxPid(fid: Int): Int

  def outerVertexGid2Vertex(gid: Long, vertex: Vertex[Long]): Boolean

  def getOEOffsetRange(startLid : Long, endLid : Long) : (Long,Long)
}
