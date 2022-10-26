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

import com.alibaba.graphscope.ds.{PropertyNbrUnit, TypedArray, Vertex}
import com.alibaba.graphscope.graphx._
import com.alibaba.graphscope.graphx.graph.GraphStructureTypes.{GraphStructureType, GraphXFragmentStructure}
import com.alibaba.graphscope.graphx.graph.{GSEdgeTripletImpl, GraphStructure, ReusableEdgeImpl}
import com.alibaba.graphscope.graphx.store.{EdgeDataStore, VertexDataStore}
import com.alibaba.graphscope.graphx.utils.{BitSetWithOffset, IdParser}
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

/** the edge array only contains out edges, we use in edge as a comparison  */
class GraphXGraphStructure(val vm : GraphXVertexMap[Long,Long], val csr : GraphXCSR[Long]) extends AbstractGraphStructure with Logging{
  val oeBeginNbr: PropertyNbrUnit[VertexId] = csr.getOEBegin(0)
  val ieBeginNbr: PropertyNbrUnit[VertexId] = csr.getIEBegin(0)
  val oeBeginAddr: Long = oeBeginNbr.getAddress
  val oeEndAddr: Long = csr.getOEEnd(vm.innerVertexSize() - 1).getAddress
  val ieBeginAddr: VertexId = ieBeginNbr.getAddress
  val ivnum: Long = vm.innerVertexSize()
  val tvnum: Long = vm.getVertexSize.toInt
  val lid2Oid: Array[TypedArray[VertexId]] = {
    val res = new Array[TypedArray[Long]](vm.fnum())
    for (i <- 0 until fnum()){
      res(i) = vm.getLid2OidAccessor(i)
    }
    res
  }
  val idParser = new IdParser(fnum())
  val outerLid2Gid: TypedArray[VertexId] = vm.getOuterLid2GidAccessor
  require(outerLid2Gid.getLength == (tvnum - ivnum), s"ovnum neq ${outerLid2Gid.getLength} vs ${tvnum - ivnum}")

  val myFid: Int = vm.fid()


  val oeOffsetsArray: TypedArray[Long] = csr.getOEOffsetsArray.asInstanceOf[TypedArray[Long]]
  val oeOffsetsLen : Long = oeOffsetsArray.getLength

  val ieOffsetsArray : TypedArray[Long] = csr.getIEOffsetsArray.asInstanceOf[TypedArray[Long]]


  @inline
  override def getOEBeginOffset(lid : Int) : Long = {
    require(lid < oeOffsetsLen, s"index out of range ${lid}, ${oeOffsetsLen}")
    oeOffsetsArray.get(lid)
  }

  @inline
  override def getIEBeginOffset(lid : Int) : Long = {
    ieOffsetsArray.get(lid)
  }

  @inline
  override def getOEEndOffset(lid : Int) : Long = {
    require(lid + 1 < oeOffsetsLen, s"index out of range ${lid}, ${oeOffsetsLen}")
    oeOffsetsArray.get(lid + 1)
  }

  @inline
  override def getIEEndOffset(lid : Int) : Long = {
    ieOffsetsArray.get(lid + 1)
  }

  @inline
  def getOutDegree(l: Int) : Long = {
    oeOffsetsArray.get(l + 1) - oeOffsetsArray.get(l)
  }

  @inline
  def getInDegree(l: Int) : Long = {
    ieOffsetsArray.get(l + 1) - ieOffsetsArray.get(l)
  }

  def outDegreeArray(startLid : Long, endLid : Long) : Array[Int] = {
    val time0 = System.nanoTime()
    val len = endLid - startLid
    val res = new Array[Int](len.toInt)
    var i = startLid.toInt
    while (i < endLid){
      res(i - startLid.toInt) = getOutDegree(i).toInt
      i += 1
    }
    val time1 = System.nanoTime()
    log.info(s"Get out degree array cost ${(time1 - time0)/1000000} ms")
    res
  }

  /** array length equal (end-start), indexed with 0 */
  def inDegreeArray(startLid : Long, endLid : Long) : Array[Int] = {
    val time0 = System.nanoTime()
    val len = endLid - startLid
    val res = new Array[Int](len.toInt)
    var i = startLid.toInt
    while (i < endLid){
      res(i - startLid.toInt) = getInDegree(i).toInt
      i += 1
    }
    val time1 = System.nanoTime()
    log.info(s"Get in degree array cost ${(time1 - time0)/1000000} ms")
    res
  }

  def inOutDegreeArray(startLid : Long, endLid : Long) : Array[Int] = {
    val len = endLid - startLid
    val res = new Array[Int](len.toInt)
    var i = startLid.toInt
    while (i < endLid) {
      res(i -startLid.toInt) = getInDegree(i).toInt + getOutDegree(i).toInt
      i += 1
    }
    res
  }

  @inline
  def getOuterVertexFid(lid: Long) : Int = {
    val gid = outerLid2Gid.get(lid - ivnum)
    idParser.getFragId(gid)
  }

  override def isInEdgesEmpty(vertex: Vertex[Long]): Boolean = csr.isInEdgesEmpty(vertex.GetValue())

  override def isOutEdgesEmpty(vertex: Vertex[Long]): Boolean = csr.isOutEdgesEmpty(vertex.GetValue())

  override def getInEdgesNum: Long = csr.getInEdgesNum

  override def getOutEdgesNum: Long = csr.getOutEdgesNum

  override def getTotalEdgesNum: VertexId = csr.getTotalEdgesNum

  override def fid(): Int = myFid

  override def fnum(): Int = vm.fnum()

  @inline
  override def getId(vertex: Vertex[Long]): Long = {
    val lid = vertex.GetValue()
    getId(lid)
  }

  @inline
  def getId(lid: VertexId): VertexId = {
    if (lid < ivnum){
      innerVertexLid2Oid(lid)
    }
    else if (lid < tvnum){
      outerVertexLid2Oid(lid)
    }
    else {
      throw new IllegalStateException(s"not possible ${lid}")
    }
  }

  override def getVertex(oid: Long, vertex: Vertex[Long]): Boolean = vm.getVertex(oid,vertex)

  override def getTotalVertexSize: Long = vm.getTotalVertexSize

  override def getVertexSize: Long = vm.getVertexSize

  override def getInnerVertexSize: Long = vm.innerVertexSize()

  override def innerVertexLid2Oid(vertex: Vertex[VertexId]): VertexId = {
    lid2Oid(myFid).get(vertex.GetValue())
  }

  def innerVertexLid2Oid(vertex : Long): VertexId = {
    lid2Oid(myFid).get(vertex)
  }

  @inline
  override def outerVertexLid2Oid(vertex: Vertex[Long]): Long = {
    val gid = outerLid2Gid.get(vertex.GetValue() - ivnum)
    val lid = idParser.getLocalId(gid)
    val dstFid = idParser.getFragId(gid)
    lid2Oid(dstFid).get(lid)
  }

  @inline
  def outerVertexLid2Oid(vertex: Long): Long = {
    val gid = outerLid2Gid.get(vertex - ivnum)
    val lid = idParser.getLocalId(gid)
    val dstFid = idParser.getFragId(gid)
    lid2Oid(dstFid).get(lid)
  }

  override def getOuterVertexSize: Long = vm.getOuterVertexSize

  override def getOuterVertexGid(vertex: Vertex[Long]): Long = {
    outerLid2Gid.get(vertex.GetValue() - ivnum)
  }

  override def fid2GraphxPid(fid: Int): Int = vm.fid2GraphxPid(fid)

  override def outerVertexGid2Vertex(gid: Long, vertex: Vertex[Long]): Boolean = vm.outerVertexGid2Vertex(gid,vertex)

  override def iterator[ED: ClassTag](startLid : Long, endLid : Long, edatas: EdgeDataStore[ED], activeEdgeSet : BitSetWithOffset, edgeReversed : Boolean = false): Iterator[Edge[ED]] = {
    getEdgeIterator(csr.getOEBegin(0), startLid,endLid,edatas,activeEdgeSet,edgeReversed)
  }

  override def tripletIterator[VD: ClassTag,ED : ClassTag](startLid : Long, endLid : Long, vertexDataStore: VertexDataStore[VD], edatas : EdgeDataStore[ED],
                                                           activeEdgeSet : BitSetWithOffset, edgeReversed : Boolean = false,
                                                           includeSrc: Boolean = true, includeDst: Boolean = true, reuseTriplet : Boolean = false, includeLid : Boolean = false)
  : Iterator[EdgeTriplet[VD, ED]] = {
    getTripletIterator(csr.getOEBegin(0),startLid,endLid,vertexDataStore,edatas,activeEdgeSet,edgeReversed,includeSrc,includeDst,reuseTriplet,includeLid)
  }

  override def getInnerVertex(oid: Long, vertex: Vertex[Long]): Boolean = {
    require(vm.getInnerVertex(oid, vertex))
    require(vertex.GetValue() < ivnum)
    true
  }

  override def getOuterVertex(oid: Long, vertex: Vertex[Long]): Boolean = {
    require(vm.getOuterVertex(oid, vertex))
    require(vertex.GetValue() >= ivnum)
    true
  }

  override val structureType: GraphStructureType = GraphXFragmentStructure

  override def getOutNbrIds(vertex: Vertex[Long]): Array[VertexId] = {
    val res = new Array[VertexId](getOutDegree(vertex).toInt)
    fillOutNbrIds(vertex.GetValue().toInt, res)
    res
  }

  def fillOutNbrIds(vid : Int, array: Array[VertexId],startInd : Int = 0) : Unit = {
    var cur = getOEBeginOffset(vid)
    val end = getOEEndOffset(vid)
    oeBeginNbr.setAddress(cur * 16 + oeBeginAddr)
    var i = startInd
    while (cur < end){
      val lid = oeBeginNbr.vid()
      array(i) = getId(lid)
      cur += 1
      i += 1
      oeBeginNbr.addV(16)
    }
  }

  override def getInNbrIds(vertex: Vertex[Long]): Array[VertexId] = {
    val res = new Array[VertexId](getInDegree(vertex).toInt)
    fillInNbrIds(vertex.GetValue().toInt, res)
    res
  }

  def fillInNbrIds(vid :Int, array : Array[VertexId], startInd : Int = 0) : Unit = {
    val beginNbr = csr.getIEBegin(vid)
    val endNbr = csr.getIEEnd(vid)
    var i = startInd
    while (beginNbr.getAddress < endNbr.getAddress){
      array(i) = getId(beginNbr.vid().toInt)
      i += 1
      beginNbr.addV(16)
    }
  }

  override def getInOutNbrIds(vertex: Vertex[Long]): Array[VertexId] = {
    val size = getInDegree(vertex) + getOutDegree(vertex)
    val res = new Array[VertexId](size.toInt)
    fillOutNbrIds(vertex.GetValue().toInt, res, 0)
    fillInNbrIds(vertex.GetValue().toInt, res, getInDegree(vertex).toInt)
    res
  }
  override def iterateTriplets[VD: ClassTag, ED: ClassTag,ED2 : ClassTag](startLid : Long, endLid : Long, f: EdgeTriplet[VD,ED] => ED2, activeVertices : BitSetWithOffset,
                                                                          vertexDataStore: VertexDataStore[VD], prevStore: EdgeDataStore[ED], activeEdgeSet: BitSetWithOffset,
                                                                          edgeReversed: Boolean, includeSrc: Boolean, includeDst: Boolean, nextStore : EdgeDataStore[ED2]): Unit = {
    var curLid = activeVertices.nextSetBit(startLid.toInt)
    val edgeTriplet = new GSEdgeTripletImpl[VD, ED]
    log.info(s"start iterating triplets, from ${startLid} to ${endLid}, ivnum ${vm.innerVertexSize()}, tvnum ${vm.getVertexSize}, oe offset len ${oeOffsetsArray.getLength}, oe offset end ${oeOffsetsArray.get(oeOffsetsLen-1)}")

    val myNbr = csr.getOEBegin(0)
    val myAddress = myNbr.getAddress
    while (curLid < endLid && curLid >= 0) {
      var curOffset = getOEBeginOffset(curLid)
      val endOffset = getOEEndOffset(curLid)
      val curAddress = curOffset * 16 + myAddress
      val endAddress = endOffset * 16 + myAddress
      myNbr.setAddress(curAddress)
      if (edgeReversed) {
        edgeTriplet.dstId = innerVertexLid2Oid(curLid)
        edgeTriplet.dstAttr = vertexDataStore.get(curLid)
      }
      else {
        edgeTriplet.srcId = innerVertexLid2Oid(curLid)
        edgeTriplet.srcAttr = vertexDataStore.get(curLid)
      }
      while (curOffset < endOffset) {
        if (activeEdgeSet.get(curOffset.toInt)) {
          val dstLid = myNbr.vid().toInt
          val eid = myNbr.eid().toInt
          if (edgeReversed) {
            edgeTriplet.srcId = getId(dstLid)
            edgeTriplet.srcAttr = vertexDataStore.get(dstLid)
          }
          else {
            edgeTriplet.dstId = getId(dstLid)
            edgeTriplet.dstAttr = vertexDataStore.get(dstLid)
          }
          edgeTriplet.attr = prevStore.getWithEID(eid)
          nextStore.setWithEID(eid, f(edgeTriplet))
        }
        myNbr.addV(16)
        curOffset += 1
      }
      curLid = activeVertices.nextSetBit(curLid + 1)
    }
  }

  override def iterateEdges[ED: ClassTag, ED2: ClassTag](startLid : Long, endLid : Long, f: Edge[ED] => ED2, prevStore : EdgeDataStore[ED], activeEdgeSet: BitSetWithOffset,
                                                         edgeReversed: Boolean, nextStore: EdgeDataStore[ED2]): Unit = {
    var curLid = startLid.toInt
    val edge = new ReusableEdgeImpl[ED]

    val myNbr = csr.getOEBegin(0)
    var curOffset = activeEdgeSet.nextSetBit(activeEdgeSet.startBit)
    var curEndOffset = getOEEndOffset(curLid)
    if (edgeReversed) {
      edge.dstId = innerVertexLid2Oid(curLid)
    }
    else {
      edge.srcId = innerVertexLid2Oid(curLid)
    }
    var stepSize = 0
    while (curOffset >= 0){
      //curLid can not be larger or equal to endLid
      val prevEndOffset = curEndOffset
      while (curOffset > curEndOffset){
        curLid += 1
        curEndOffset = getOEEndOffset(curLid)
      }
      if (prevEndOffset != curEndOffset){//which means curLid has been changed.
        if (edgeReversed) {
          edge.dstId = innerVertexLid2Oid(curLid)
        }
        else {
          edge.srcId = innerVertexLid2Oid(curLid)
        }
      }
      myNbr.addV(stepSize * 16)
      val dstLid = myNbr.vid().toInt
      val eid = myNbr.eid().toInt
      if (edgeReversed) {
        edge.srcId = getId(dstLid)
      }
      else {
        edge.dstId = getId(dstLid)
      }
      edge.attr = prevStore.getWithEID(eid)
      nextStore.setWithEID(eid, f(edge))

      val nextOffset = activeEdgeSet.nextSetBit(curOffset + 1)
      stepSize = nextOffset - curOffset
      curOffset = nextOffset
    }
  }

  override def getOEOffsetRange(startLid: VertexId, endLid: VertexId): (VertexId, VertexId) = {
    (csr.getOEOffset(startLid),csr.getOEOffset(endLid))
  }

  override def getInDegree(vertex: Vertex[VertexId]): VertexId = {
    getInDegree(vertex.GetValue().toInt)
  }

  override def getOutDegree(vertex: Vertex[VertexId]): VertexId = {
    getOutDegree(vertex.GetValue().toInt)
  }
}

