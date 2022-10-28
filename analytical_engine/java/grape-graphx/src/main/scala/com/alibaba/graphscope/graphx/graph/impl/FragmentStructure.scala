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
import com.alibaba.graphscope.fragment.adaptor.{
  AbstractArrowProjectedAdaptor,
  ArrowProjectedAdaptor,
  ArrowProjectedStringEDAdaptor,
  ArrowProjectedStringVDAdaptor,
  ArrowProjectedStringVEDAdaptor
}
import com.alibaba.graphscope.fragment.mapper.ArrowProjectedFragmentMapper
import com.alibaba.graphscope.fragment.{
  ArrowProjectedFragment,
  BaseArrowProjectedFragment,
  FragmentType,
  IFragment
}
import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.graph.GraphStructureTypes.{ArrowProjectedStructure, GraphStructureType}
import com.alibaba.graphscope.graphx.graph.{GSEdgeTripletImpl, ReusableEdgeImpl}
import com.alibaba.graphscope.graphx.store.{EdgeDataStore, VertexDataStore}
import com.alibaba.graphscope.graphx.utils.{BitSetWithOffset, GrapeUtils, ScalaFFIFactory}
import com.alibaba.graphscope.utils.{FFITypeFactoryhelper, LongPointerAccessor}
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

/** Although fragment structure is independent of vertex data type and edge data type, but we still need to store
  * the vd and ed class tag, in case we want to map to a new Fragment.
  * @param fragment
  * @param vdClass
  * @param edClass
  */
class FragmentStructure(
    val fragment: IFragment[Long, Long, _, _]
) extends AbstractGraphStructure
    with Logging
    with Serializable {
  override val structureType: GraphStructureType = ArrowProjectedStructure
  val fid2Pid                                    = new Array[Int](fragment.fnum())
  val ivnum                                      = fragment.getInnerVerticesNum

  var oePtr, iePtr: PropertyNbrUnit[Long]                         = null
  var oePtrStartAddr, iePtrStartAddr: Long                        = 0
  var oeOffsetBeginArray, ieOffsetBeginArray: LongPointerAccessor = null
  var oeOffsetEndArray, ieOffsetEndArray: LongPointerAccessor     = null
  var fragEdgeNum                                                 = 0

  if (fragment.fragmentType().equals(FragmentType.ArrowProjectedFragment)) {
    val projectedFragment = fragment
      .asInstanceOf[AbstractArrowProjectedAdaptor[Long, Long, _, _]]
      .getBaseArrayProjectedFragment
      .asInstanceOf[ArrowProjectedFragment[Long, Long, _, _]]
    oePtr = projectedFragment.getOutEdgesPtr
    iePtr = projectedFragment.getInEdgesPtr
    oePtrStartAddr = oePtr.getAddress
    iePtrStartAddr = iePtr.getAddress
    oeOffsetBeginArray = new LongPointerAccessor(projectedFragment.getOEOffsetsBeginPtr)
    ieOffsetBeginArray = new LongPointerAccessor(projectedFragment.getIEOffsetsBeginPtr)
    oeOffsetEndArray = new LongPointerAccessor(projectedFragment.getOEOffsetsEndPtr)
    ieOffsetEndArray = new LongPointerAccessor(projectedFragment.getIEOffsetsEndPtr)
    fragEdgeNum = projectedFragment.getEdgeNum.toInt // count different from grape fragment.
  } else {
    throw new IllegalStateException(s"not supported type ${fragment.fragmentType()}")
  }

  val startLid     = 0
  val endLid: Long = fragment.getInnerVerticesNum()
  log.info(
    s"Creating graphStructure@${hashCode()}, vertices (${startLid},${endLid}), edges ${fragEdgeNum} out edges ${getOutEdgesNum} "
  )

  @inline
  override def getIEBeginOffset(lid: Int): Long = {
    ieOffsetBeginArray.get(lid)
  }

  @inline
  override def getIEEndOffset(lid: Int): Long = {
    ieOffsetEndArray.get(lid + 1)
  }

  def outDegreeArray(startLid: Long, endLid: Long): Array[Int] = {
    val time0  = System.nanoTime()
    val len    = fragment.getVerticesNum.toInt
    val res    = new Array[Int](len)
    var i      = startLid.toInt
    val vertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
    while (i < endLid) {
      vertex.SetValue(i)
      res(i) = getOutDegree(vertex).toInt
      i += 1
    }
    while (i < len) {
      res(i) = 0
      i += 1
    }
    val time1 = System.nanoTime()
    log.info(s"Get out degree array cost ${(time1 - time0) / 1000000} ms")
    res
  }

  def inDegreeArray(startLid: Long, endLid: Long): Array[Int] = {
    val time0  = System.nanoTime()
    val len    = fragment.getVerticesNum.toInt
    val res    = new Array[Int](len)
    var i      = startLid.toInt
    val vertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
    while (i < endLid) {
      vertex.SetValue(i)
      res(i) = getInDegree(vertex).toInt
      i += 1
    }
    while (i < len) {
      res(i) = 0
      i += 1
    }
    val time1 = System.nanoTime()
    log.info(s"Get in degree array cost ${(time1 - time0) / 1000000} ms")
    res
  }

  def inOutDegreeArray(startLid: Long, endLid: Long): Array[Int] = {
    val len    = fragment.getVerticesNum.toInt
    val res    = new Array[Int](len)
    var i      = 0
    val vertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
    while (i < endLid) {
      vertex.SetValue(i)
      res(i) = getInDegree(vertex).toInt + getOutDegree(vertex).toInt
      i += 1
    }
    while (i < len) {
      res(i) = 0
      i += 1
    }
    res
  }

  override def isInEdgesEmpty(vertex: Vertex[Long]): Boolean = {
    fragment.getLocalInDegree(vertex) == 0
  }

  override def isOutEdgesEmpty(vertex: Vertex[Long]): Boolean = {
    fragment.getLocalOutDegree(vertex) == 0
  }

  override def getInEdgesNum: Long = fragment.getInEdgeNum

  override def getOutEdgesNum: Long = fragment.getOutEdgeNum

  override def getTotalEdgesNum: VertexId = fragEdgeNum

  override def fid(): Int = fragment.fid()

  override def fnum(): Int = fragment.fnum()

  override def getVertex(oid: Long, vertex: Vertex[Long]): Boolean = {
    fragment.getVertex(oid, vertex)
  }

  override def getTotalVertexSize: Long = fragment.getTotalVerticesNum

  override def getVertexSize: Long = fragment.getVerticesNum

  override def getInnerVertexSize: Long = fragment.getInnerVerticesNum

  override def outerVertexLid2Oid(vertex: Vertex[VertexId]): Long = {
    fragment.getOuterVertexId(vertex)
  }

  override def getOuterVertexSize: Long = fragment.getOuterVerticesNum

  override def getOuterVertexGid(vertex: Vertex[Long]): Long = {
    fragment.getOuterVertexGid(vertex)
  }

  def initFid2GraphxPid(array: Array[(PartitionID, Int)]): Unit = {
    for (tuple <- array) {
      fid2Pid(tuple._2) = tuple._1
    }
    log.info(s"Filled fid2 graphx pid ${fid2Pid.mkString("Array(", ", ", ")")}")
  }

  override def fid2GraphxPid(fid: Int): Int = {
    fid2Pid(fid)
  }

  override def outerVertexGid2Vertex(
      gid: Long,
      vertex: Vertex[Long]
  ): Boolean = {
    fragment.outerVertexGid2Vertex(gid, vertex)
  }

  /** For us, the input edatas should be null, and we shall not reply on it to get edge data. */
  override def iterator[ED: ClassTag](
      startLid: Long,
      endLid: Long,
      edatas: EdgeDataStore[ED],
      activeSet: BitSetWithOffset,
      edgeReversed: Boolean
  ): Iterator[Edge[ED]] = {
    if (fragment.fragmentType().equals(FragmentType.ArrowProjectedFragment)) {
      val projectedFragment =
        fragment.asInstanceOf[AbstractArrowProjectedAdaptor[Long, Long, _, ED]]
      newProjectedIterator(
        startLid,
        endLid,
        projectedFragment.getBaseArrayProjectedFragment
          .asInstanceOf[BaseArrowProjectedFragment[Long, Long, _, ED]],
        edatas,
        activeSet,
        edgeReversed
      )
    } else {
      throw new IllegalStateException("Not implemented")
    }
  }

  private def newProjectedIterator[ED: ClassTag](
      startLid: Long,
      endLid: Long,
      frag: BaseArrowProjectedFragment[Long, Long, _, ED],
      edatas: EdgeDataStore[ED],
      activeEdgeSet: BitSetWithOffset,
      edgeReverse: Boolean
  ): Iterator[Edge[ED]] = {
    getEdgeIterator(
      frag.getOutEdgesPtr,
      startLid,
      endLid,
      edatas,
      activeEdgeSet,
      edgeReverse
    )
  }

  override def tripletIterator[VD: ClassTag, ED: ClassTag](
      startLid: Long,
      endLid: Long,
      innerVertexDataStore: VertexDataStore[VD],
      edatas: EdgeDataStore[ED],
      activeSet: BitSetWithOffset,
      edgeReversed: Boolean = false,
      includeSrc: Boolean = true,
      includeDst: Boolean = true,
      reuseTriplet: Boolean = false,
      includeLid: Boolean = false
  ): Iterator[EdgeTriplet[VD, ED]] = {
    if (fragment.fragmentType().equals(FragmentType.ArrowProjectedFragment)) {
      val projectedFragment = fragment
        .asInstanceOf[AbstractArrowProjectedAdaptor[Long, Long, VD, ED]]
        .getBaseArrayProjectedFragment
        .asInstanceOf[BaseArrowProjectedFragment[Long, Long, VD, ED]]
      log.info(
        s"creating triplet iterator v2 with java edata, with inner vd store ${innerVertexDataStore}"
      )
      newProjectedTripletIterator(
        startLid,
        endLid,
        projectedFragment,
        innerVertexDataStore,
        edatas,
        activeSet,
        edgeReversed,
        includeSrc,
        includeDst,
        reuseTriplet,
        includeLid
      )
    } else {
      throw new IllegalStateException("Not implemented")
    }
  }

  private def newProjectedTripletIterator[VD: ClassTag, ED: ClassTag](
      startLid: Long,
      endLid: Long,
      frag: BaseArrowProjectedFragment[Long, Long, VD, ED],
      vertexDataStore: VertexDataStore[VD],
      edatas: EdgeDataStore[ED],
      activeEdgeSet: BitSetWithOffset,
      edgeReversed: Boolean,
      includeSrc: Boolean,
      includeDst: Boolean,
      reuseTriplet: Boolean,
      includeLid: Boolean = false
  ): Iterator[EdgeTriplet[VD, ED]] = {
    getTripletIterator(
      frag.getOutEdgesPtr,
      startLid,
      endLid,
      vertexDataStore,
      edatas,
      activeEdgeSet,
      edgeReversed,
      includeSrc,
      includeDst,
      reuseTriplet,
      includeLid
    )
  }

  override def getInnerVertex(oid: Long, vertex: Vertex[Long]): Boolean = {
    require(fragment.getInnerVertex(oid, vertex))
    true
  }

  override def getOuterVertex(oid: Long, vertex: Vertex[Long]): Boolean = {
    require(fragment.getOuterVertex(oid, vertex))
    true
  }

  override def getOutNbrIds(vertex: Vertex[Long]): Array[VertexId] = {
    val size = getOutDegree(vertex)
    val res  = new Array[VertexId](size.toInt)
    fillOutNbrIdsImpl(vertex.GetValue(), res)
    res
  }

  override def getInNbrIds(vertex: Vertex[Long]): Array[VertexId] = {
    val size = getInDegree(vertex)
    val res  = new Array[VertexId](size.toInt)
    fillInNbrIdsImpl(vertex.GetValue().toInt, res)
    res
  }

  override def getInDegree(vertex: Vertex[Long]): Long = {
    fragment.getLocalInDegree(vertex)
  }

  def fillInNbrIdsImpl(
      vid: Int,
      array: Array[VertexId],
      startInd: Int = 0
  ): Unit = {
    var curOff = ieOffsetBeginArray.get(vid)
    val endOff = ieOffsetEndArray.get(vid)
    iePtr.setAddress(iePtrStartAddr + curOff * 16)
    var i      = startInd
    val vertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
    while (curOff < endOff) {
      vertex.SetValue(iePtr.vid())
      val dstOid = getId(vertex)
      array(i) = dstOid
      curOff += 1
      i += 1
      iePtr.addV(16)
    }
  }

  override def getInOutNbrIds(vertex: Vertex[Long]): Array[VertexId] = {
    val size = getInDegree(vertex) + getOutDegree(vertex)
    val res  = new Array[VertexId](size.toInt)
    val vid  = vertex.GetValue()
    fillOutNbrIdsImpl(vid, res, 0)
    fillInNbrIdsImpl(vid.toInt, res, getOutDegree(vertex).toInt)
    res
  }

  override def getOutDegree(vertex: Vertex[Long]): Long = {
    fragment.getLocalOutDegree(vertex)
  }

  def fillOutNbrIdsImpl(
      vid: VertexId,
      array: Array[VertexId],
      startInd: Int = 0
  ): Unit = {
    var curOff = oeOffsetBeginArray.get(vid)
    val endOff = oeOffsetEndArray.get(vid)
    oePtr.setAddress(oePtrStartAddr + curOff * 16)
    var i      = startInd
    val vertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
    while (curOff < endOff) {
      vertex.SetValue(oePtr.vid())
      val dstOid = getId(vertex)
      array(i) = dstOid
      curOff += 1
      i += 1
      oePtr.addV(16)
    }
  }

  override def iterateTriplets[VD: ClassTag, ED: ClassTag, ED2: ClassTag](
      startLid: Long,
      endLid: Long,
      f: EdgeTriplet[VD, ED] => ED2,
      activeVertices: BitSetWithOffset,
      innerVertexDataStore: VertexDataStore[VD],
      edatas: EdgeDataStore[ED],
      activeSet: BitSetWithOffset,
      edgeReversed: Boolean,
      includeSrc: Boolean,
      includeDst: Boolean,
      newArray: EdgeDataStore[ED2]
  ): Unit = {
    if (fragment.fragmentType().equals(FragmentType.ArrowProjectedFragment)) {
      val projectedFragment = fragment
        .asInstanceOf[AbstractArrowProjectedAdaptor[Long, Long, VD, ED]]
        .getBaseArrayProjectedFragment
        .asInstanceOf[BaseArrowProjectedFragment[Long, Long, VD, ED]]
      iterateProjectedTriplets(
        projectedFragment,
        startLid,
        endLid,
        f,
        activeVertices,
        innerVertexDataStore,
        edatas,
        activeSet,
        edgeReversed,
        includeSrc,
        includeDst,
        newArray
      )
    } else {
      throw new IllegalStateException("Not implemented")
    }
  }

  def iterateProjectedTriplets[VD: ClassTag, ED: ClassTag, ED2: ClassTag](
      frag: BaseArrowProjectedFragment[Long, Long, VD, ED],
      startLid: Long,
      endLid: Long,
      f: EdgeTriplet[VD, ED] => ED2,
      activeVertices: BitSetWithOffset,
      vertexDataStore: VertexDataStore[VD],
      prevStore: EdgeDataStore[ED],
      activeSet: BitSetWithOffset,
      edgeReversed: Boolean,
      includeSrc: Boolean,
      includeDst: Boolean,
      nextStore: EdgeDataStore[ED2]
  ): Unit = {
    var curLid      = activeVertices.nextSetBit(startLid.toInt)
    val edgeTriplet = new GSEdgeTripletImpl[VD, ED]
    log.info(
      s"start iterating triplets, from ${startLid} to ${endLid}, ivnum ${frag.getInnerVerticesNum}, tvnum ${frag.getVerticesNum}"
    )

    val myNbr     = frag.getOutEdgesPtr
    val myAddress = myNbr.getAddress
    val vertex    = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
    while (curLid < endLid && curLid >= 0) {
      val curAddress = getOEBeginOffset(curLid) * 16 + myAddress
      val endAddress = getOEEndOffset(curLid) * 16 + myAddress
      myNbr.setAddress(curAddress)
      vertex.SetValue(curLid)
      if (edgeReversed) {
        edgeTriplet.dstId = innerVertexLid2Oid(vertex)
        edgeTriplet.dstAttr = vertexDataStore.get(curLid)
      } else {
        edgeTriplet.srcId = innerVertexLid2Oid(vertex)
        edgeTriplet.srcAttr = vertexDataStore.get(curLid)
      }
      while (myNbr.getAddress < endAddress) {
        val dstLid = myNbr.vid().toInt
        val eid    = myNbr.eid().toInt
        vertex.SetValue(dstLid)
        if (edgeReversed) {
          edgeTriplet.srcId = getId(vertex)
          edgeTriplet.srcAttr = vertexDataStore.get(dstLid)
        } else {
          edgeTriplet.dstId = getId(vertex)
          edgeTriplet.dstAttr = vertexDataStore.get(dstLid)
        }
        edgeTriplet.attr = prevStore.getWithEID(eid)
        nextStore.setWithEID(eid, f(edgeTriplet))
        myNbr.addV(16)
      }
      curLid = activeVertices.nextSetBit(curLid + 1)
    }
  }

  override def getId(vertex: Vertex[VertexId]): VertexId = {
    fragment.getId(vertex)
  }

  @inline
  override def getOEBeginOffset(lid: Int): Long = {
    oeOffsetBeginArray.get(lid)
  }

  @inline
  override def getOEEndOffset(lid: Int): Long = {
    oeOffsetEndArray.get(lid)
  }

  override def innerVertexLid2Oid(vertex: Vertex[VertexId]): VertexId = {
    fragment.getInnerVertexId(vertex)
  }

  override def iterateEdges[ED: ClassTag, ED2: ClassTag](
      startLid: Long,
      endLid: Long,
      f: Edge[ED] => ED2,
      edatas: EdgeDataStore[ED],
      activeSet: BitSetWithOffset,
      edgeReversed: Boolean,
      newArray: EdgeDataStore[ED2]
  ): Unit = {
    if (fragment.fragmentType().equals(FragmentType.ArrowProjectedFragment)) {
      val projectedFragment = fragment
        .asInstanceOf[AbstractArrowProjectedAdaptor[Long, Long, _, ED]]
        .getBaseArrayProjectedFragment
        .asInstanceOf[BaseArrowProjectedFragment[Long, Long, _, ED]]
      iterateProjectedEdges(
        projectedFragment,
        startLid,
        endLid,
        f,
        edatas,
        activeSet,
        edgeReversed,
        newArray
      )
    } else {
      throw new IllegalStateException("Not implemented")
    }
  }

  def iterateProjectedEdges[ED: ClassTag, ED2: ClassTag](
      frag: BaseArrowProjectedFragment[Long, Long, _, ED],
      startLid: Long,
      endLid: Long,
      f: Edge[ED] => ED2,
      prevStore: EdgeDataStore[ED],
      activeSet: BitSetWithOffset,
      edgeReversed: Boolean,
      nextStore: EdgeDataStore[ED2]
  ): Unit = {

    var curLid    = startLid.toInt
    val edge      = new ReusableEdgeImpl[ED]
    val myNbr     = frag.getOutEdgesPtr
    val myAddress = myNbr.getAddress
    val vertex    = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
    while (curLid < endLid && curLid >= 0) {
      val curAddress = getOEBeginOffset(curLid) * 16 + myAddress
      val endAddress = getOEEndOffset(curLid) * 16 + myAddress
      myNbr.setAddress(curAddress)
      vertex.SetValue(curLid)
      if (edgeReversed) {
        edge.dstId = innerVertexLid2Oid(vertex)
      } else {
        edge.srcId = innerVertexLid2Oid(vertex)
      }
      while (myNbr.getAddress < endAddress) {
        val dstLid = myNbr.vid().toInt
        val eid    = myNbr.eid().toInt
        vertex.SetValue(dstLid)
        if (edgeReversed) {
          edge.srcId = getId(vertex)
        } else {
          edge.dstId = getId(vertex)
        }
        edge.attr = prevStore.getWithEID(eid)
        nextStore.setWithEID(eid, f(edge))
        myNbr.addV(16)
      }
      curLid += 1
    }
  }

  override def getOEOffsetRange(
      startLid: VertexId,
      endLid: VertexId
  ): (VertexId, VertexId) = {
    (oeOffsetBeginArray.get(startLid), oeOffsetEndArray.get(endLid - 1))
  }

  def mapToNewFragImp[VD: ClassTag, ED: ClassTag](
      vertexDataStore: VertexDataStore[VD],
      edgeDataStore: EdgeDataStore[ED],
      client: VineyardClient
  ): String = {

    if (GrapeUtils.isPrimitive[VD] && GrapeUtils.isPrimitive[ED]) {
      val vertexDataArrayBuilder = GrapeUtils.vertexDataStore2ArrowArrayBuilder(vertexDataStore, ivnum.toInt)
      val edgeDataArrayBuilder   = GrapeUtils.edgeDataStore2ArrowArrayBuilder(edgeDataStore)
      val castedFrag = fragment.asInstanceOf[ArrowProjectedAdaptor[Long, Long, _, _]].getArrowProjectedFragment
      val mapper     = ScalaFFIFactory.newProjectedFragmentMapper[VD, ED]
      if (edgeDataArrayBuilder == null) {
        mapper
          .map(
            castedFrag.getArrowFragment,
            castedFrag.vertexLabel(),
            castedFrag.edgePropId(),
            vertexDataArrayBuilder,
            client
          )
          .get()
          .id()
          .toString()
      } else {
        mapper
          .map(
            castedFrag.getArrowFragment,
            castedFrag.vertexLabel(),
            castedFrag.edgeLabel(),
            vertexDataArrayBuilder,
            edgeDataArrayBuilder,
            client
          )
          .get()
          .id()
          .toString()
      }
    } else if (!GrapeUtils.isPrimitive[VD] && GrapeUtils.isPrimitive[ED]) {
      val vertexDataArrayBuilder = GrapeUtils.vertexDataStore2ArrowStringArrayBuilder(vertexDataStore, ivnum.toInt)
      val edgeDataArrayBuilder   = GrapeUtils.edgeDataStore2ArrowArrayBuilder(edgeDataStore)
      val castedFrag =
        fragment.asInstanceOf[ArrowProjectedStringVDAdaptor[Long, Long, _]].getArrowProjectedStrVDFragment
      val mapper = ScalaFFIFactory.newProjectedStringVDFragmentMapper[VD, ED]
      if (edgeDataArrayBuilder == null) {
        mapper
          .map(
            castedFrag.getArrowFragment,
            castedFrag.vertexLabel(),
            castedFrag.edgePropId(),
            vertexDataArrayBuilder,
            client
          )
          .get()
          .id()
          .toString()
      } else {
        mapper
          .map(
            castedFrag.getArrowFragment,
            castedFrag.vertexLabel(),
            castedFrag.edgeLabel(),
            vertexDataArrayBuilder,
            edgeDataArrayBuilder,
            client
          )
          .get()
          .id()
          .toString()
      }
    } else if (GrapeUtils.isPrimitive[VD] && !GrapeUtils.isPrimitive[ED]) {
      val vertexDataArrayBuilder = GrapeUtils.vertexDataStore2ArrowArrayBuilder(vertexDataStore, ivnum.toInt)
      val edgeDataArrayBuilder   = GrapeUtils.edgeDataStore2ArrowStringArrayBuilder(edgeDataStore)
      val castedFrag =
        fragment.asInstanceOf[ArrowProjectedStringEDAdaptor[Long, Long, _]].getArrowProjectedStrEDFragment
      val mapper = ScalaFFIFactory.newProjectedStringEDFragmentMapper[VD, ED]
      if (edgeDataArrayBuilder == null) {
        mapper
          .map(
            castedFrag.getArrowFragment,
            castedFrag.vertexLabel(),
            castedFrag.edgePropId(),
            vertexDataArrayBuilder,
            client
          )
          .get()
          .id()
          .toString()
      } else {
        mapper
          .map(
            castedFrag.getArrowFragment,
            castedFrag.vertexLabel(),
            castedFrag.edgeLabel(),
            vertexDataArrayBuilder,
            edgeDataArrayBuilder,
            client
          )
          .get()
          .id()
          .toString()
      }
    } else {
      val vertexDataArrayBuilder = GrapeUtils.vertexDataStore2ArrowStringArrayBuilder(vertexDataStore, ivnum.toInt)
      val edgeDataArrayBuilder   = GrapeUtils.edgeDataStore2ArrowStringArrayBuilder(edgeDataStore)
      val castedFrag =
        fragment.asInstanceOf[ArrowProjectedStringVEDAdaptor[Long, Long]].getArrowProjectedStrVEDFragment
      val mapper = ScalaFFIFactory.newProjectedStringVEDFragmentMapper[VD, ED]
      if (edgeDataArrayBuilder == null) {
        mapper
          .map(
            castedFrag.getArrowFragment,
            castedFrag.vertexLabel(),
            castedFrag.edgePropId(),
            vertexDataArrayBuilder,
            client
          )
          .get()
          .id()
          .toString()
      } else {
        mapper
          .map(
            castedFrag.getArrowFragment,
            castedFrag.vertexLabel(),
            castedFrag.edgeLabel(),
            vertexDataArrayBuilder,
            edgeDataArrayBuilder,
            client
          )
          .get()
          .id()
          .toString()
      }
    }

  }

  /** Construct a new fragment with new vd and ed, return the frag id */
  def mapToNewFragment[VD: ClassTag, ED: ClassTag](
      vertexDataStore: VertexDataStore[VD],
      edgeDataStore: EdgeDataStore[ED],
      client: VineyardClient
  ): String = {
    mapToNewFragImp(vertexDataStore, edgeDataStore, client)
  }
}

object FragmentStructure {
  val NBR_SIZE = 16
}
