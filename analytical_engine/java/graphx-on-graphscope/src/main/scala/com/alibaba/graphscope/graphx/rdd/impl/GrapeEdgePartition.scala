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

package com.alibaba.graphscope.graphx.rdd.impl

import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder
import com.alibaba.graphscope.ds.{PropertyNbrUnit, Vertex}
import com.alibaba.graphscope.graphx._
import com.alibaba.graphscope.graphx.graph.{GSEdgeTriplet, GraphStructure, ReusableEdge}
import com.alibaba.graphscope.graphx.shuffle.{EdgeShuffleReceived, EdgeShuffle}
import com.alibaba.graphscope.graphx.store.impl.{InHeapEdgeDataStore, OffHeapEdgeDataStore}
import com.alibaba.graphscope.graphx.store.{EdgeDataStore, VertexDataStore}
import com.alibaba.graphscope.graphx.utils._
import com.alibaba.graphscope.stdcxx.StdVector
import com.alibaba.graphscope.utils.{FFITypeFactoryhelper, ThreadSafeBitSet}
import org.apache.spark.Partition
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.{BitSet, OpenHashSet}

import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, CountDownLatch, Executors, TimeUnit}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * [startLid, endLid), endLid is exclusive
 */
class GrapeEdgePartition[VD: ClassTag, ED: ClassTag](val pid : Int,
                                                     val localId : Int, // the order of partition in this fragment.
                                                     val localNum : Int, // how many part in this frag.
                                                     val siblingPid : Array[Int],
                                                     val startLid : Long,
                                                     val endLid : Long,
                                                     var activeEdgeNum : Int = 0, //use a instant variable to accelerate count.
                                                     val graphStructure: GraphStructure,
                                                     val client : VineyardClient,
                                                     var edatas : EdgeDataStore[ED],
                                                     val edgeReversed : Boolean = false,
                                                     var activeEdgeSet : BitSetWithOffset = null,
                                                     var treeMaps : Array[java.util.TreeMap[Long,Int]] = null) extends Logging  with Partition{

  val getVertexSize = endLid - startLid

  /** regard to our design, the card(activeEdgeSet) == partOutEdgeNum, len(edatas) == allEdgesNum, indexed by eid */
  val partOutEdgeNum : Long = graphStructure.getOutEdgesNum
  val partInEdgeNum : Long = graphStructure.getInEdgesNum
  val totalFragEdgeNum : Long = graphStructure.getTotalEdgesNum


  if (activeEdgeSet == null){
    val (startOffset,endOffset) = graphStructure.getOEOffsetRange(startLid,endLid)
    activeEdgeSet = new BitSetWithOffset(startBit = startOffset.toInt,endBit = endOffset.toInt)
    activeEdgeSet.setRange(startOffset.toInt, endOffset.toInt)
    activeEdgeNum = endOffset.toInt - startOffset.toInt
    log.info(s"part ${pid} local id ${localId}/${localNum} edges range ${startOffset} to ${endOffset}, cardinality ${activeEdgeSet.cardinality()}, ${activeEdgeSet}")
  }
  val NBR_SIZE = 16L
  //to avoid the difficult to get srcLid in iterating over edges.

  // (graphx pid, fid, start lid, end lid)
  def buildPartitionInfo(infoArray : Array[(Int,Int,Long,Long)]) : Unit = {
    require(treeMaps == null, "try to build partition info from non empty")
    treeMaps = new Array[java.util.TreeMap[Long,Int]](graphStructure.fnum())
    for (i <- 0 until graphStructure.fnum()){
      treeMaps(i) = new util.TreeMap[Long,Int]()
    }
    for (value <- infoArray){
      val graphxPid = value._1
      val graphxFid = value._2
      val startLid = value._3
      treeMaps(graphxFid).put(startLid, graphxPid)
    }
  }

  //array of length fnum, each contains the local id of vertex we need as outer vertex.
  def generateRoutingMessage() : Array[Array[Long]] = {
    val ovnum = graphStructure.getOuterVertexSize
    val ivnum = graphStructure.getInnerVertexSize
    val chunkSize = (ovnum + localNum - 1) / localNum
    val begin = ivnum + (chunkSize * localId)
    val end = Math.min(begin + chunkSize, ovnum + ivnum)
    val partitionNum = {
      treeMaps.map(_.size()).sum
    }
    if (localId==0) log.info(s"partition num ${partitionNum}")
    val res = Array.fill(partitionNum)(new PrimitiveVector[Long]())

    val idParser = new IdParser(graphStructure.fnum())
    var i = begin
    log.info(s"in generating routing message, from ${pid}, range ${begin}, ${end}")
    val vertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
    while (i < end){
      vertex.SetValue(i)
      val gid = graphStructure.getOuterVertexGid(vertex)
      val lid = idParser.getLocalId(gid)
      val fid = idParser.getFragId(gid)
      val dstPid = treeMaps(fid).floorEntry(lid).getValue
      res(dstPid).+=(lid)
      i += 1
    }
    res.map(_.trim().array)
  }


  def getDegreeArray(edgeDirection: EdgeDirection): Array[Int] = {
    if (edgeDirection.equals(EdgeDirection.In)){
      graphStructure.inDegreeArray(startLid,endLid)
    }
    else if (edgeDirection.equals(EdgeDirection.Out)){
      graphStructure.outDegreeArray(startLid,endLid)
    }
    else{
      graphStructure.inOutDegreeArray(startLid,endLid)
    }
  }

  /** Iterate over out edges only, in edges will be iterated in other partition */
  def iterator : Iterator[Edge[ED]] = {
    graphStructure.iterator(startLid, endLid,edatas,activeEdgeSet,edgeReversed)
  }

  def tripletIterator(innerVertexDataStore: VertexDataStore[VD],
                      includeSrc: Boolean = true, includeDst: Boolean = true, reuseTriplet : Boolean = false, includeLid : Boolean = false)
  : Iterator[EdgeTriplet[VD, ED]] = graphStructure.tripletIterator(startLid,endLid,innerVertexDataStore,edatas,activeEdgeSet,edgeReversed,includeSrc,includeDst, reuseTriplet, includeLid)

  def filter(
              epred: EdgeTriplet[VD, ED] => Boolean,
              vpred: (VertexId, VD) => Boolean,
              innerVertexDataStore: VertexDataStore[VD]): GrapeEdgePartition[VD, ED] = {
    val tripletIter = tripletIterator(innerVertexDataStore, true, true, true).asInstanceOf[Iterator[GSEdgeTriplet[VD,ED]]]
    val newActiveEdges = new BitSetWithOffset(activeEdgeSet.startBit,activeEdgeSet.endBit)
    newActiveEdges.union(activeEdgeSet)
    while (tripletIter.hasNext){
      val triplet = tripletIter.next()
      if (!vpred(triplet.srcId,triplet.srcAttr) || !vpred(triplet.dstId, triplet.dstAttr) || !epred(triplet)){
        newActiveEdges.unset(triplet.offset.toInt)
      }
    }
    this.withNewMask(newActiveEdges)
  }

  def createNewValues[ED2 : ClassTag] :EdgeDataStore[ED2] = {
    if (localId == 0){
      val newValues = edatas.mapToNew[ED2]
      log.info(s"pid ${pid} create new EdgeStore for part ${siblingPid.mkString(",")}, new class ${GrapeUtils.getRuntimeClass[ED2].getSimpleName}")
      for (dstPid <- siblingPid){
        EdgeDataStore.enqueue(dstPid, newValues)
      }
    }
    EdgeDataStore.dequeue(pid).asInstanceOf[EdgeDataStore[ED2]]
  }

  def groupEdges(merge: (ED, ED) => ED): GrapeEdgePartition[VD, ED] = {
    val iter = iterator.asInstanceOf[Iterator[ReusableEdge[ED]]]
    var curSrcId = -1L
    var curDstId = -1L
    var prevEdgeInd = -1L // when we find a new one, we unset prevEdgeInd
    var attrSum = null.asInstanceOf[ED]
    val newMask = new BitSetWithOffset(activeEdgeSet.startBit,activeEdgeSet.endBit)
    newMask.union(activeEdgeSet)
    val newEdata = createNewValues[ED]
    while (iter.hasNext){
      val edge = iter.next()
      val curIndex = edge.offset
      var flag = false
      if (flag && edge.srcId == curSrcId && edge.dstId == curDstId){
        attrSum = merge(attrSum, edge.attr)
        newMask.unset(prevEdgeInd.toInt)
        prevEdgeInd = curIndex
        log.info(s"Merge edge (${curSrcId}, ${curDstId}), val ${attrSum}")
      }
      else {
        if (flag){
          //a new round start, we add new Edata to this edge
          newEdata.setWithOffset(prevEdgeInd.toInt, attrSum)
          log.info(s"end of acculating edge ${curSrcId}, ${curDstId}, ${attrSum}")
        }
        curSrcId = edge.srcId
        curDstId = edge.dstId
        prevEdgeInd = curIndex
        attrSum = edge.attr
      }
      flag = true
    }
    new GrapeEdgePartition[VD,ED](pid,localId,localNum,siblingPid, startLid, endLid, newMask.cardinality(), graphStructure, client,newEdata, edgeReversed, newMask)
  }

  def map[ED2: ClassTag](f: Edge[ED] => ED2): GrapeEdgePartition[VD, ED2] = {
    val newData = createNewValues[ED2]
    graphStructure.iterateEdges(startLid, endLid,f, edatas, activeEdgeSet, edgeReversed, newData)
    this.withNewEdata(newData)
  }

  def map[ED2: ClassTag](f: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2]): GrapeEdgePartition[VD, ED2] = {
    val newData = createNewValues[ED2]
    val iter = iterator.asInstanceOf[Iterator[ReusableEdge[ED]]]
    val resultEdata = f(pid, iter)
    var ind = activeEdgeSet.nextSetBit(activeEdgeSet.startBit)
    while (ind >= 0 && resultEdata.hasNext){
      newData.setWithOffset(ind,resultEdata.next())
      ind = activeEdgeSet.nextSetBit(ind + 1)
    }
    if (resultEdata.hasNext || ind >= 0){
      throw new IllegalStateException(s"impossible, two iterator should end at the same time ${ind}, ${resultEdata.hasNext}")
    }
    this.withNewEdata(newData)
  }

  def mapTriplets[ED2: ClassTag](f: EdgeTriplet[VD,ED] => ED2,activeVertices: BitSetWithOffset, innerVertexDataStore: VertexDataStore[VD], tripletFields: TripletFields): GrapeEdgePartition[VD, ED2] = {
    val time0 = System.nanoTime()
    val newData = createNewValues[ED2]
    log.info(s"${this.toString} got new edata ${newData}")
    val time01 = System.nanoTime()
    graphStructure.iterateTriplets(startLid, endLid, f, activeVertices,innerVertexDataStore, edatas, activeEdgeSet, edgeReversed,tripletFields.useSrc, tripletFields.useDst, newData)
    val time1 = System.nanoTime()
    log.info(s"[Perf:] part ${pid}, lid ${localId}, mapping over triplets size ${activeEdgeNum} cost ${(time1 - time0)/1000000} ms, create array cost ${(time01 - time0)/1000000}ms")
    this.withNewEdata(newData)
  }

  def mapTriplets[ED2: ClassTag](f: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2], innerVertexDataStore: VertexDataStore[VD], includeSrc  : Boolean = true, includeDst : Boolean = true): GrapeEdgePartition[VD, ED2] = {
    val newData = createNewValues[ED2]
    val iter = tripletIterator(innerVertexDataStore,includeSrc,includeDst,reuseTriplet = true).asInstanceOf[Iterator[GSEdgeTriplet[VD,ED]]]
    val resultEdata = f(pid, iter)
    var ind = activeEdgeSet.nextSetBit(activeEdgeSet.startBit)
    while (ind >= 0 && resultEdata.hasNext){
      newData.setWithOffset(ind,resultEdata.next())
      ind = activeEdgeSet.nextSetBit(ind + 1)
    }
    if (ind >=0 || resultEdata.hasNext){
      throw new IllegalStateException(s"impossible, two iterator should end at the same time ${ind}, ${resultEdata.hasNext}")
    }
    this.withNewEdata(newData)
  }

  def scanEdgeTriplet[A: ClassTag](innerVertexDataStore: VertexDataStore[VD], sendMsg: EdgeContext[VD, ED, A] => Unit, mergeMsg: (A, A) => A, tripletFields: TripletFields, directionOpt : Option[(EdgeDirection)]) : Iterator[(VertexId, A)] ={
    val aggregates = new Array[A](innerVertexDataStore.size)
    val bitset = new BitSet(aggregates.length)

    val ctx = new EdgeContextImpl[VD, ED, A](mergeMsg, aggregates, bitset)
    val tripletIter = tripletIterator(innerVertexDataStore, tripletFields.useSrc, tripletFields.useDst, reuseTriplet = true,includeLid = true).asInstanceOf[Iterator[GSEdgeTriplet[VD,ED]]]
    while (tripletIter.hasNext){
      val triplet = tripletIter.next()
      ctx.set(triplet.srcId, triplet.dstId, triplet.srcLid.toInt, triplet.dstLid.toInt, triplet.srcAttr,triplet.dstAttr, triplet.attr)
      sendMsg(ctx)
    }

    val curFid = graphStructure.fid()
    val idParser = new IdParser(graphStructure.fnum())
    bitset.iterator.map( localId => (idParser.generateGlobalId(curFid, localId), aggregates(localId)))
  }

  def reverse: GrapeEdgePartition[VD, ED] = {
    new GrapeEdgePartition[VD,ED](pid, localId,localNum, siblingPid,startLid,endLid,activeEdgeNum, graphStructure, client,edatas,!edgeReversed, activeEdgeSet)
  }

  def withNewEdata[ED2: ClassTag](newEdata : EdgeDataStore[ED2]): GrapeEdgePartition[VD, ED2] = {
    new GrapeEdgePartition[VD,ED2](pid,localId,localNum, siblingPid,startLid,endLid,activeEdgeNum,  graphStructure, client,newEdata, edgeReversed, activeEdgeSet)
  }

  def withNewMask(newActiveSet: BitSetWithOffset) : GrapeEdgePartition[VD,ED] = {
    new GrapeEdgePartition[VD,ED](pid,localId,localNum,siblingPid,startLid,endLid, newActiveSet.cardinality(), graphStructure, client,edatas, edgeReversed, newActiveSet)
  }

  /**  currently we only support inner join with same vertex map*/
  def innerJoin[ED2: ClassTag, ED3: ClassTag]
  (other: GrapeEdgePartition[_, ED2])
  (f: (VertexId, VertexId, ED, ED2) => ED3): GrapeEdgePartition[VD, ED3] = {
    if (this.graphStructure != other.graphStructure){
      throw new IllegalStateException("Currently we only support inner join with same index")
    }
    val newMask = this.activeEdgeSet & other.activeEdgeSet
    val newActiveEdgesNum = newMask.cardinality()
    log.info(s"Inner join edgePartition 0 has ${this.activeEdgeSet.cardinality()} actives edges, the other has ${other.activeEdgeSet} active edges")
    log.info(s"after join ${newMask.cardinality()} active edges")
    val newEData = createNewValues[ED3]
    val oldIter = iterator.asInstanceOf[Iterator[ReusableEdge[ED]]]
    while (oldIter.hasNext){
      val oldEdge = oldIter.next()
      val oldIndex = oldEdge.offset.toInt
      if (newMask.get(oldIndex)){
        newEData.setWithOffset(oldEdge.offset.toInt, f(oldEdge.srcId, oldEdge.dstId, oldEdge.attr, other.edatas.getWithOffset(oldEdge.offset.toInt)))
      }
    }

    new GrapeEdgePartition[VD,ED3](pid,localId,localNum, siblingPid,startLid,endLid, newActiveEdgesNum, graphStructure, client,newEData,edgeReversed, newMask)
  }

  override def toString: String =  super.toString + "(pid=" + pid + ",local id" + localId +
    ", start lid" + startLid + ", end lid " + endLid + ",graph structure" + graphStructure +
    ",out edges num" + partOutEdgeNum + ", in edges num" + partInEdgeNum  + ", data store: " + edatas +")"

  override def index: PartitionID = pid
}

class GrapeEdgePartitionBuilder[VD: ClassTag, ED: ClassTag](val numPartitions : Int,val client : VineyardClient) extends Logging{
  var receivedShuffles : Array[EdgeShuffleReceived[ED]] = null.asInstanceOf[Array[EdgeShuffleReceived[ED]]]
  def addEdges(in : Array[EdgeShuffleReceived[ED]]) : Unit = {
    receivedShuffles = in
  }

  /**
   * outer set must be java.util.Set to be thread safe.
   */
  def collectOids(inner: OpenHashSet[Long], outer: java.util.Set[Long], shuffles: ArrayBuffer[EdgeShuffle[_, _]], cores: Int, shuffleSize : Int) : Unit = {
    val time0 = System.nanoTime()
    val atomicInt = new AtomicInteger(0)
    val threads = new Array[Thread](cores)
    val numShuffles = shuffles.length
    log.info(s"process ${numShuffles} shuffles with ${cores} thread")
    var tid = 0
    while (tid < cores) {
      val newThread = new Thread() {
        override def run(): Unit = {
          var flag = true
          while (flag) {
            val got = atomicInt.getAndAdd(1);
            if (got >= numShuffles) {
              flag = false
            }
            else {
              val edgeShuffle = shuffles(got)
              if (edgeShuffle.srcs != null) {
                  var i = 0
                  val receivedDst = edgeShuffle.dsts
                  while (i < receivedDst.length) {
                    val oid = receivedDst(i)
                    if (!inner.contains(oid)) {
                      outer.add(oid)
                    }
                    i += 1
                  }
              }
            }
          }
        }
      }
      newThread.start()
      threads(tid) = newThread
      tid += 1
    }
    for (i <- 0 until cores) {
      threads(i).join()
    }
    log.info(s"after remove iv in outer, size ${outer.size}")

    val time1 = System.nanoTime()
    log.info(s"[Collect oids cost ${(time1 - time0)/1000000}ms]")
  }

  def mergeInnerOpenHashSet(oidSets : ArrayBuffer[OpenHashSet[Long]]) : OpenHashSet[Long] = {
    val t0 = System.nanoTime()
    val queue = new ArrayBlockingQueue[OpenHashSet[Long]](8 * oidSets.length)
    for (i <- oidSets.indices){
      require(queue.offer(oidSets(i)))
    }
    val coreNum = Math.min(queue.size(), 64)
    log.info(s"using ${coreNum} threads for local inner vertex join, queue size ${queue.size()}")
    val threads = new Array[Thread](coreNum)
    for (i <- 0 until  coreNum) {
      threads(i) = new Thread(){
        override def run() : Unit = {
          while (queue.size() > 1){
            val tuple = this.synchronized {
              if (queue.size() > 1) {
                val first = queue.take()
                val second = queue.take()
                if (first.size < second.size) {
                  (second, first)
                }
                else (first, second)
              }
              else null
            }
            if (tuple != null) {
              require(queue.offer(tuple._1.union(tuple._2)))
            }
          }
        }
      }
      threads(i).start()
    }
    for (i <- 0 until coreNum){
      threads(i).join()
    }
    require(queue.size() == 1)
    val t1 = System.nanoTime()
    log.info(s"merge openHashSet cost ${(t1 - t0)/1000000} ms")
    queue.take()
  }

  def mergeInnerOids(oidsSet : ArrayBuffer[Array[Long]]) : OpenHashSet[Long] = {
    val t0 = System.nanoTime()
    val rawSize = oidsSet.map(_.size).sum
    val consumerNum = 64
    val consumers = new Array[Thread](consumerNum)
    val index = new AtomicInteger(0)
    val numArrays = oidsSet.length

    val coalesce = 4
    val initSize = rawSize / (30 * coalesce)
    log.info(s"parallel processing inner oids arr length ${numArrays}, raw size ${rawSize}, set init size ${initSize}")

    val tmpSet = new Array[OpenHashSet[Long]](coalesce)
    for (i <- tmpSet.indices){
      tmpSet(i) =  new OpenHashSet[Long](initSize)
    }
    for (i <- 0 until  consumerNum) {
      consumers(i) = new Thread(){
        var flag = true
        while (flag){
          val cur = index.getAndAdd(1)
          if (cur >= numArrays){
            flag = false
          }
          else {
            val curArray = oidsSet(cur)
            val iter= curArray.iterator
            val set = tmpSet(cur % coalesce)
            while (iter.hasNext){
              val value = iter.next()
              if (!set.contains(value)){
                set.add(value)
              }
            }
          }
        }
      }
      consumers(i).start()
    }
    for (i <- 0 until consumerNum){
      consumers(i).join()
    }

    val t01 = System.nanoTime()
    var result = tmpSet(0)
    for (i <- 1 until coalesce){
      result = result.union(tmpSet(i))
    }

    val t1 = System.nanoTime()
    log.info(s"merge inner oid cost ${(t01 - t0)/1000000} ms, merge result set ${(t1 - t01)/1000000} ms")
    result
  }

  /**
   * @return the built local vertex map id.
   */
  def buildLocalVertexMap(pid : Int, parallelism : Int, shufflePidArr : Array[Int], shuffleSize : Int) : LocalVertexMap[Long,Long] = {
    //We need to get oid->lid mappings in this executor.
    val time0 = System.nanoTime()
    val edgeShuffles = new ArrayBuffer[EdgeShuffle[_, _]]()

    for (receive <- receivedShuffles) {
      for (edgeShuffle <- receive.fromPid2Shuffle) {
        if (edgeShuffle != null) {
          edgeShuffles.+=(edgeShuffle)
        }
      }
    }
    val innerOidsArray = edgeShuffles.filter(_.oidArray != null).map(_.oidArray)
    val innerOids : OpenHashSet[Long] = {
      if (innerOidsArray.length == 0) {
        log.info("Building inner oid vertex set from [set]")
        val openHashSets = edgeShuffles.filter(_.oidSet != null).map(_.oidSet)
        require(openHashSets.length > 0, "set array should empty")
        mergeInnerOpenHashSet(openHashSets)
      }
      else {
        log.info("Building inner oid vertex set from [array]")
        mergeInnerOids(innerOidsArray)
      }
    }

    log.info(s"Inner oids ${innerOids.size}")

    val outerOids = ConcurrentHashMap.newKeySet[Long](innerOids.size)
    collectOids(innerOids, outerOids, edgeShuffles, parallelism, shuffleSize)

    log.info(s"Found totally inner ${innerOids.size}, outer ${outerOids.size()} in ${ExecutorUtils.getHostName}:${pid}")
    if (innerOids.size == 0 && outerOids.size == 0){
      log.info(s"partition ${pid} empty")
      return null
    }
    val innerOidBuilder : ArrowArrayBuilder[Long] = ScalaFFIFactory.newSignedLongArrayBuilder()
    val outerOidBuilder : ArrowArrayBuilder[Long] = ScalaFFIFactory.newSignedLongArrayBuilder()
    val pidBuilder : ArrowArrayBuilder[Int] = ScalaFFIFactory.newSignedIntArrayBuilder()
    val time01 = System.nanoTime()

    innerOidBuilder.reserve(innerOids.size)
    //count size
    var innerSize = 0
    val innerIter1 = innerOids.iterator
    while (innerIter1.hasNext){
      val _ = innerIter1.next()
      innerSize += 1
    }
    require(innerSize == innerOids.size, s"neq ${innerSize}, ${innerOids.size}")
    val tmp = innerOids.iterator
    while (tmp.hasNext){
      innerOidBuilder.unsafeAppend(tmp.next())
    }

    outerOidBuilder.reserve(outerOids.size)
    //count size
    var outerSize = 0
    val outerIter1 = outerOids.iterator
    while (outerIter1.hasNext){
      val _ = outerIter1.next()
      outerSize += 1
    }
    require(outerSize == outerOids.size, s"neq ${outerSize}, ${outerOids.size}")
    val outerIter = outerOids.iterator
    while (outerIter.hasNext){
      outerOidBuilder.unsafeAppend(outerIter.next())
    }
    pidBuilder.reserve(shufflePidArr.length)
    for (v <- shufflePidArr){
      pidBuilder.unsafeAppend(v)
    }
    val time1 = System.nanoTime()
    val localVertexMapBuilder = ScalaFFIFactory.newLocalVertexMapBuilder(client, innerOidBuilder, outerOidBuilder, pidBuilder)
    val localVM = localVertexMapBuilder.seal(client).get()
    val time2 = System.nanoTime()
    log.info(s"${ExecutorUtils.getHostName}: Finish building local vm: ${localVM.id()}, ${localVM.getInnerVerticesNum}, " +
      s"select out vertices cost ${(time01 - time0) / 1000000} ms adding buf take ${(time1 - time01)/ 1000000} ms," +
      s"total building time ${(time2 - time0)/ 1000000} ms")
    localVM
  }

  /** The received edata arrays contains both in edges and out edges, but we only need these out edges's edata array */
  def buildEdataStore(defaultED : ED, totalEdgeNum : Int, client : VineyardClient, eidAccessor : EIDAccessor, localNum : Int = 1) : EdgeDataStore[ED] = {
    val eDataStore = if (GrapeUtils.isPrimitive[ED]){
      val edataBuilder = ScalaFFIFactory.newEdgeDataBuilder[ED](client,totalEdgeNum)
      new OffHeapEdgeDataStore[ED](totalEdgeNum, localNum,client,eidAccessor, edataBuilder)
    }
    else {
      val edataArray = buildArrayStore(defaultED,totalEdgeNum)
      new InHeapEdgeDataStore[ED](totalEdgeNum, localNum, client, edataArray,eidAccessor)
    }
    fillEdataStore(defaultED,totalEdgeNum,eDataStore)
    eDataStore
  }

  def buildArrayStore(defaultED : ED, totalEdgeNum : Long) : Array[ED] = {
    val firstValue = receivedShuffles(0)
    if (defaultED != null && firstValue.getArrays._3(0) == null){
      Array.fill[ED](totalEdgeNum.toInt)(defaultED)
    }
    else if (defaultED == null && firstValue.getArrays._3(0) != null){
      val allArrays = receivedShuffles.flatMap(_.getArrays._3)
      val rawEdgesNum = allArrays.map(_.length).sum
      require(rawEdgesNum == totalEdgeNum, s"edge num neq ${rawEdgesNum}, ${totalEdgeNum}")
      val edataArray = new Array[ED](rawEdgesNum)
      //flat array
      var ind = 0
      for (arr <- allArrays){
        var i = 0
        val t = arr.length
        while (i < t){
          edataArray(ind) = arr(i)
          i += 1
          ind += 1
        }
      }
      edataArray
    }
    else {
      throw new IllegalStateException("not possible, default ed and array is both null or bot not empty")
    }
  }
  def fillEdataStore(defaultED : ED, totalEdgeNum : Int, edataStore : EdgeDataStore[ED]) : Unit = {
    val valueIterator = receivedShuffles.toIterator
    val firstValue = valueIterator.next()
    if (defaultED != null && firstValue.getArrays._3(0) == null){
      var i = 0
      while (i < totalEdgeNum){
        edataStore.setWithEID(i, defaultED)
        i += 1
      }
    }
    else if (defaultED == null && firstValue.getArrays._3(0) != null){
      log.info("Filling edge data array with shuffle edatas")
      val allArrays = receivedShuffles.flatMap(_.getArrays._3)
      val rawEdgesNum = allArrays.map(_.length).sum
      require(rawEdgesNum == totalEdgeNum, s"edge num neq ${rawEdgesNum}, ${totalEdgeNum}")
      var ind = 0
      for (arr <- allArrays){
        var i = 0
        val t = arr.length
        while (i < t){
          edataStore.setWithEID(ind, arr(i))
          i += 1
          ind += 1
        }
      }
    }
    else {
      throw new IllegalStateException("not possible, default ed and array is both null or bot not empty")
    }
  }

  def createEids(edgeNum : Int, oeBeginNbr : PropertyNbrUnit[Long]) : Array[Long] = {
    val res = new Array[Long](edgeNum)
    var i = 0
    while (i < edgeNum){
      res(i) = oeBeginNbr.eid()
      oeBeginNbr.addV(16)
      i += 1
    }
    res
  }

  // no edata building is needed, we only persist edata to c++ when we run pregel
  def buildCSR(globalVMID : Long, localNum : Int = 1): (GraphXVertexMap[Long,Long], GraphXCSR[Long]) = {
    val time0 = System.nanoTime()
    log.info(s"Constructing csr with global vm ${globalVMID}")
    val graphxVertexMapGetter = ScalaFFIFactory.newVertexMapGetter()
    val graphxVertexMap = graphxVertexMapGetter.get(client, globalVMID).get()
    log.info(s"Got graphx vertex map: ${graphxVertexMap}, total vnum ${graphxVertexMap.getTotalVertexSize}, fid ${graphxVertexMap.fid()}/${graphxVertexMap.fnum()}")
    val cores = Runtime.getRuntime.availableProcessors();
    log.info(s"per partition parallel: ${ cores / localNum}")

    val (srcOids, dstOids) = preprocessEdges(cores / localNum)
    val distinctNum = srcOids.size()

    log.info("Finish adding edges to builders")
    val graphxCSRBuilder = ScalaFFIFactory.newGraphXCSRBuilder(client)
    require(srcOids.size() == distinctNum && dstOids.size() == distinctNum, s"src size ${srcOids.size()} dst size ${dstOids.size()}, edgenum ${distinctNum}")
    graphxCSRBuilder.loadEdges(srcOids,dstOids,graphxVertexMap, localNum)
    val graphxCSR = graphxCSRBuilder.seal(client).get()
    val time1 = System.nanoTime()
    log.info(s"Finish building graphx csr ${graphxVertexMap.fid()}, cost ${(time1 - time0)/1000000} ms")
    (graphxVertexMap,graphxCSR)
  }

  def preprocessEdges(cores : Int) : (StdVector[Long],StdVector[Long]) = {
    val rawEdgesNum = receivedShuffles.map(_.totalSize()).sum
    log.info(s"Got totally shuffle ${receivedShuffles.size}, edges ${rawEdgesNum} in ${ExecutorUtils.getHostName}")
    val srcOids = ScalaFFIFactory.newLongVector
    val dstOids = ScalaFFIFactory.newLongVector
    srcOids.resize(rawEdgesNum)
    dstOids.resize(rawEdgesNum)
    val time0 = System.nanoTime()

    val (srcArrays, dstArrays) = (receivedShuffles.flatMap(_.getArrays._1).toArray,receivedShuffles.flatMap(_.getArrays._2))
    var tid = 0
    val atomicInt = new AtomicInteger(0)
    val outerSize = srcArrays.length
    log.info(s"use ${cores} threads for ${outerSize} shuffles")
    val threads = new Array[Thread](cores)
    val curInd = new AtomicInteger(0)
    while (tid < cores) {
      val newThread = new Thread() {
        override def run(): Unit = {
          var flag = true
          while (flag) {
            var (got,myBegin) = synchronized{
              val got = atomicInt.getAndAdd(1);
              if (got >= outerSize) (-1,-1)
              else {
                val myBegin = curInd.getAndAdd(srcArrays(got).length)
                (got, myBegin)
              }
            }
            if (got < 0) {
              flag = false
            }
            else {
              val innerLimit = srcArrays(got).length
              require(dstArrays(got).length == innerLimit)
              val srcArray = srcArrays(got)
              val dstArray = dstArrays(got)
              val end = myBegin + innerLimit
              var innerInd = 0
              while (myBegin < end){
                srcOids.set(myBegin, srcArray(innerInd))
                dstOids.set(myBegin, dstArray(innerInd))
                myBegin += 1
                innerInd += 1
              }
            }
          }
        }
      }
      newThread.start()
      threads(tid) = newThread
      tid += 1
    }
    for (i <- 0 until cores) {
      threads(i).join()
    }
    require(curInd.get() == rawEdgesNum, s"neq ${curInd.get()} ${rawEdgesNum}")

    val time1 = System.nanoTime()
    log.info(s"preprocess edge array cost ${(time1 - time0)/1000000}ms")
    (srcOids,dstOids)
  }

  def fillVertexData(innerVertexData : VertexDataStore[VD], graphStructure: GraphStructure) : Boolean = {
    val time0 = System.nanoTime()
    var flag = false
    for (shuffleReceived <- receivedShuffles){
      for (shuffle <- shuffleReceived.fromPid2Shuffle){
        val edgeShuffle = shuffle.asInstanceOf[EdgeShuffle[VD,_]]
        val innerOids = edgeShuffle.oidArray
        val innerVertexAttrs = edgeShuffle.vertexAttrs
        if (innerVertexAttrs == null || innerVertexAttrs.length == 0){
        }
        else {
          val limit = innerOids.size
          val grapeVertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
          require(limit == innerVertexAttrs.length, s"oid size and vertex attr size neq ${limit}, ${innerVertexAttrs.length}")
          val ivnum = graphStructure.getInnerVertexSize.toInt
          var i = 0
          while (i < limit){
            val oid = innerOids(i)
            val vdata = innerVertexAttrs(i)
            require(graphStructure.getInnerVertex(oid,grapeVertex))
            val lid = grapeVertex.GetValue().toInt
            require(lid < ivnum, s"expect no outer vertex ${lid}, ${ivnum}")
            innerVertexData.set(lid, vdata)
            i += 1
          }
          flag = true
        }
      }
    }
    val time1 = System.nanoTime()
    if (flag) {
      log.info(s"[Perf] filling vertex data cost ${(time1 - time0) / 1000000} ms")
    }
    else {
      log.info("[Perf] no vertex data in shuffle.")
    }
    flag
  }

  //call this to delete c++ ptr and release memory of arrow builders
  def clearBuilders() : Unit = {
    receivedShuffles = null
  }

}
object GrapeEdgePartition extends Logging {
  val tupleQueue = new ArrayBlockingQueue[(Int,GraphStructure,VineyardClient,EdgeDataStore[_],VertexDataStore[_], Boolean)](1024)
  val pidQueue = new ArrayBlockingQueue[Int](1024)
  var pid2EdgePartition = null.asInstanceOf[mutable.HashMap[Int,GrapeEdgePartition[_,_]]]

  implicit def convert(a : (Long,Long)) = new OrderedLongLong(a)

  def push(in : (Int,GraphStructure,VineyardClient,EdgeDataStore[_],VertexDataStore[_], Boolean)): Unit = {
    require(tupleQueue.offer(in))
  }

  def incCount(pid : Int) : Unit = synchronized{
    require(pidQueue.offer(pid))
  }

  def createPartitions[VD: ClassTag, ED: ClassTag](pid : Int, totalPartitionNum : Int) : Unit = synchronized{
    if (pid2EdgePartition == null){
      synchronized {
        if (pid2EdgePartition == null){
          val size = tupleQueue.size()
          pid2EdgePartition = new mutable.HashMap[Int,GrapeEdgePartition[VD,ED]].asInstanceOf[mutable.HashMap[Int,GrapeEdgePartition[_,_]]]
          if (size == pidQueue.size()){
            log.info(s"Totally $size ele in queue, registered partition num ${pidQueue.size()}")
            for (_ <- 0 until size){
              val tuple = tupleQueue.poll()
              val edataStore = tuple._4.asInstanceOf[EdgeDataStore[ED]]
              edataStore.setLocalNum(1)
              val vertexDataStore = tuple._5
              vertexDataStore.setLocalNum(1)
              val _ = pidQueue.poll()
              val siblings = new Array[Int](1)
              siblings(0) = tuple._1
              pid2EdgePartition(tuple._1) = new GrapeEdgePartition[VD,ED](tuple._1, 0, 1, siblings,0,  tuple._2.getInnerVertexSize,tuple._2.getOutEdgesNum.toInt, tuple._2, tuple._3, edataStore)
              GrapeVertexPartition.setVertexStore(tuple._1, vertexDataStore, tuple._6)
            }
          }
          else {
            //find out the new partition ids which didn't appear in frag queue.
            val candidates = new mutable.Queue[Int]
            val tuplePids = new BitSet(totalPartitionNum)
            tupleQueue.forEach(tuple => tuplePids.set(tuple._1))
            pidQueue.forEach(p => if (!tuplePids.get(p)) candidates.enqueue(p))
            log.info(s"candidates num ${candidates.size}")
            require(candidates.size == (pidQueue.size() - tupleQueue.size()), s"candidates size ${candidates.size}, neq ${pidQueue.size()} - ${tupleQueue.size()}")
            val registeredNum = pidQueue.size()
            val maxTimes = (registeredNum + size - 1) / size // the largest split num
            val numLargestSplit = (registeredNum - (maxTimes - 1) * size)

            log.info(s"Totally ${size} ele in queue, registered partition num ${pidQueue.size()}, first ${numLargestSplit} frags are splited into ${maxTimes}, others are splited into ${maxTimes - 1} times")
            for (i <- 0 until size){
              val tuple = tupleQueue.poll()
              val totalIvnum = tuple._2.getInnerVertexSize
              val times = {
                if (i < numLargestSplit) maxTimes
                else maxTimes - 1
              }

              val graphStructure = tuple._2
              val innerStore = tuple._5
              innerStore.setLocalNum(times)
              val edataStore = tuple._4.asInstanceOf[EdgeDataStore[ED]]
              edataStore.setLocalNum(times)
              //split into partitions where each partition has rarely even number of edges.
              val ranges = splitFragAccordingToEdges(graphStructure, times)
              val siblingPids = new Array[Int](times)
              siblingPids(0) = tuple._1
              for (j <- 1 until times){
                siblingPids(j) = candidates.dequeue()
              }
              for (j <- 0 until times){
                val startLid = ranges(j)._1
                val endLid = ranges(j)._2
                if (j == times - 1){
                  require(endLid == totalIvnum)
                }
                if (j == 0){
                  pid2EdgePartition(tuple._1) = new GrapeEdgePartition[VD,ED](tuple._1,  j, times,siblingPids, startLid, endLid,0, graphStructure, tuple._3,edataStore)
                  GrapeVertexPartition.setVertexStore(tuple._1, innerStore,tuple._6)
                  log.info(s"creating partition for pid ${tuple._1}, (${startLid},${endLid}), fid ${graphStructure.fid()}")
                }
                else {
                  val dstPid = siblingPids(j)
                  pid2EdgePartition(dstPid) = new GrapeEdgePartition[VD,ED](dstPid,  j, times, siblingPids, startLid, endLid,0, graphStructure, tuple._3, edataStore)
                  GrapeVertexPartition.setVertexStore(dstPid, innerStore,tuple._6)
                  log.info(s"creating partition for pid ${dstPid}, (${startLid},${endLid}), fid ${graphStructure.fid()}")
                }
              }
            }
          }
          require(tupleQueue.size() == 0, "queue should be empty now")
        }
        else {
          log.info(s"partition ${pid} skip building since array is already created")
        }
      }
    }
  }

  def get[VD: ClassTag, ED: ClassTag](pid : Int) : GrapeEdgePartition[VD,ED] = synchronized{
    require(pid2EdgePartition != null, "call create partitions first")
    pid2EdgePartition(pid).asInstanceOf[GrapeEdgePartition[VD,ED]]
  }

  def splitFragAccordingToEdges(graphStructure: GraphStructure, numPart : Int) : Array[(Int,Int)] = {
    val numEdges = graphStructure.getOutEdgesNum
    val edgesPerSplit = (numEdges + numPart - 1) / numPart
    var curLid = 0
    val res = new Array[(Int,Int)](numPart)
    for (i <- 0 until numPart){
      val targetOffset = Math.min(edgesPerSplit * (i + 1), numEdges)
      val beginLid = curLid
      while (graphStructure.getOEBeginOffset(curLid) < targetOffset){
        curLid += 1
      }
      if (i == numPart - 1){
        curLid = Math.max(curLid, graphStructure.getInnerVertexSize.toInt)
      }
      res(i) = (beginLid, curLid)
      log.info(s"For part ${i}, startLid ${beginLid}, endLid${curLid}, num edges in this part ${graphStructure.getOEBeginOffset(curLid) - graphStructure.getOEBeginOffset(beginLid)}, total edges ${numEdges}, edges per split ${edgesPerSplit}")
    }
    //it is possible that the last vertices has no out edges.
    require(curLid == graphStructure.getInnerVertexSize, s"after split, should iterate over all ivertex ${curLid}, ${graphStructure.getInnerVertexSize}")
    res
  }
}

private class EdgeContextImpl[VD, ED, A](
                                                 mergeMsg: (A, A) => A,
                                                 aggregates: Array[A],
                                                 bitset: BitSet)
  extends EdgeContext[VD, ED, A] {

  private[this] var _srcId: VertexId = _
  private[this] var _dstId: VertexId = _
  private[this] var _localSrcId: Int = _
  private[this] var _localDstId: Int = _
  private[this] var _srcAttr: VD = _
  private[this] var _dstAttr: VD = _
  private[this] var _attr: ED = _

  def set(
           srcId: VertexId, dstId: VertexId,
           localSrcId: Int, localDstId: Int,
           srcAttr: VD, dstAttr: VD,
           attr: ED): Unit = {
    _srcId = srcId
    _dstId = dstId
    _localSrcId = localSrcId
    _localDstId = localDstId
    _srcAttr = srcAttr
    _dstAttr = dstAttr
    _attr = attr
  }

  def setSrcOnly(srcId: VertexId, localSrcId: Int, srcAttr: VD): Unit = {
    _srcId = srcId
    _localSrcId = localSrcId
    _srcAttr = srcAttr
  }

  def setDest(dstId: VertexId, localDstId: Int, dstAttr: VD, attr: ED): Unit = {
    _dstId = dstId
    _localDstId = localDstId
    _dstAttr = dstAttr
    _attr = attr
  }

  override def srcId: VertexId = _srcId
  override def dstId: VertexId = _dstId
  override def srcAttr: VD = _srcAttr
  override def dstAttr: VD = _dstAttr
  override def attr: ED = _attr

  override def sendToSrc(msg: A): Unit = {
    send(_localSrcId, msg)
  }
  override def sendToDst(msg: A): Unit = {
    send(_localDstId, msg)
  }

  @inline private def send(localId: Int, msg: A): Unit = {
    if (bitset.get(localId)) {
      aggregates(localId) = mergeMsg(aggregates(localId), msg)
    } else {
      aggregates(localId) = msg
      bitset.set(localId)
    }
  }
}


class OrderedLongLong(val tuple : (Long,Long)) extends Ordered[(Long,Long)] {
  override def compare(that: (Long, Long)): Int = {
    if (tuple._1 == that._1) return (tuple._2 - that._2).toInt
    else return (tuple._1 - that._1).toInt
  }
}

