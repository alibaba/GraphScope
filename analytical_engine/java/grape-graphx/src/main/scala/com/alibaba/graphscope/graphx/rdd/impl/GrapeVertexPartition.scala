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

import com.alibaba.graphscope.ds.{PrimitiveTypedArray, StringTypedArray, Vertex}
import com.alibaba.graphscope.fragment.ArrowProjectedFragment
import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.graph.GraphStructure
import com.alibaba.graphscope.graphx.rdd.RoutingTable
import com.alibaba.graphscope.graphx.store.VertexDataStore
import com.alibaba.graphscope.graphx.store.impl.AbstractVertexDataStore
import com.alibaba.graphscope.graphx.utils._
import com.alibaba.graphscope.serialization.FakeFFIByteVectorInputStream
import com.alibaba.graphscope.stdcxx.FakeFFIByteVector
import com.alibaba.graphscope.utils.{FFITypeFactoryhelper, VertexDataUtils}
import org.apache.spark.Partition
import org.apache.spark.graphx.{EdgeDirection, PartitionID, VertexId}
import org.apache.spark.internal.Logging

import java.io.{BufferedReader, FileReader, ObjectInputStream}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class VertexDataMessage[VD: ClassTag](
    val dstPid: Int,
    val fromFid: Int,
    val gids: Array[Long],
    val newData: Array[VD]
) extends Serializable

class GrapeVertexPartition[VD: ClassTag](
    val pid: Int,
    val startLid: Int,
    val endLid: Int,
    val localId: Int,
    val localNum: Int,
    val siblingPid: Array[Int],
    val graphStructure: GraphStructure,
    val vertexData: VertexDataStore[VD],
    val client: VineyardClient,
    val routingTable: RoutingTable,
    var bitSet: BitSetWithOffset = null
) extends Logging
    with Partition {

  val partVnum: Long = endLid - startLid
  val fragVnum: Int  = graphStructure.getVertexSize.toInt
  val ivnum          = graphStructure.getInnerVertexSize.toInt
  val vertex         = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
  if (bitSet == null) {
    bitSet = new BitSetWithOffset(startBit = startLid, endBit = endLid)
    bitSet.setRange(startLid, endLid)
  }

  def iterator: Iterator[(VertexId, VD)] = {
    new Iterator[(VertexId, VD)] {
      var lid = bitSet.nextSetBit(startLid)
      val vertex =
        FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
      override def hasNext: Boolean = {
        lid >= 0 && lid < endLid
      }

      override def next(): (VertexId, VD) = {
        vertex.setValue(lid)
        val res = (graphStructure.innerVertexLid2Oid(vertex), getData(lid))
        lid = bitSet.nextSetBit(lid + 1)
        res
      }
    }
  }

  def collectNbrIds(
      edgeDirection: EdgeDirection
  ): GrapeVertexPartition[Array[VertexId]] = {
    var lid       = bitSet.nextSetBit(startLid)
    val newValues = createNewValues[Array[VertexId]]

    val vertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
    while (lid >= 0 && lid < endLid) {
      vertex.setValue(lid)
      newValues.set(lid, getNbrIds(vertex, edgeDirection))
      lid = bitSet.nextSetBit(lid + 1);
    }
    this.withNewValues(newValues)
  }

  def getNbrIds(
      vertex: Vertex[Long],
      edgeDirection: EdgeDirection
  ): Array[Long] = {
    if (edgeDirection.equals(EdgeDirection.In)) {
      graphStructure.getInNbrIds(vertex)
    } else if (edgeDirection.equals(EdgeDirection.Out)) {
      graphStructure.getOutNbrIds(vertex)
    } else if (edgeDirection.equals(EdgeDirection.Either)) {
      graphStructure.getInOutNbrIds(vertex)
    } else {
      throw new IllegalStateException(
        s"Not supported direction ${edgeDirection.toString()}"
      )
    }
  }

  def createNewValues[VD2: ClassTag]: VertexDataStore[VD2] = {
    if (localId == 0) {
      val newValues =
        vertexData.mapToNew[VD2]
      log.info(
        s"pid ${pid} create new values for part ${siblingPid
          .mkString(",")}, new class ${GrapeUtils.getRuntimeClass[VD2].getSimpleName}"
      )
      for (dstPid <- siblingPid) {
        VertexDataStore.enqueue(dstPid, newValues)
      }
    }
    VertexDataStore.dequeue(pid).asInstanceOf[VertexDataStore[VD2]]
  }

  def generateVertexDataMessage: Iterator[(PartitionID, VertexDataMessage[VD])] = {
    val res      = new ArrayBuffer[(PartitionID, VertexDataMessage[VD])]()
    val curFid   = graphStructure.fid()
    val idParser = new IdParser(graphStructure.fnum())
    for (i <- 0 until (routingTable.numPartitions)) {
      val verticesToSend = routingTable.get(i)
      if (verticesToSend != null) {
        val gids    = new PrimitiveVector[Long](verticesToSend.size)
        val newData = new PrimitiveVector[VD](verticesToSend.size)
        var j       = verticesToSend.nextSetBit(startLid)
        while (j >= 0 && j < endLid) {
          gids.+=(idParser.generateGlobalId(curFid, j))
          newData.+=(getData(j))
          require(
            getData(j) != null,
            s"generating vd msg encounter null at pos ${j}"
          )
          j = verticesToSend.nextSetBit(j + 1)
        }
        val msg = new VertexDataMessage[VD](
          i,
          curFid,
          gids.trim().array,
          newData.trim().array
        )
        res.+=((i, msg))
      }
    }
    res.toIterator
  }

  def getData(lid: Int): VD = {
    vertexData.get(lid)
  }

  def updateOuterVertexData(
      vertexDataMessage: Iterator[(PartitionID, VertexDataMessage[VD])]
  ): GrapeVertexPartition[VD] = {
    val time0 = System.nanoTime()
    log.info(s"Start updating outer vertex on part ${pid}")
    val tmpVertex =
      FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
    if (vertexDataMessage.hasNext) {
      var tid = 0
      while (vertexDataMessage.hasNext) {
        val tuple      = vertexDataMessage.next()
        val outerDatas = tuple._2.newData
        val outerGids  = tuple._2.gids
        var i          = 0
        while (i < outerGids.length) {
          require(graphStructure.outerVertexGid2Vertex(outerGids(i), tmpVertex))
          require(
            outerDatas(i) != null,
            s"received null msg in ${tuple}, pos ${i}"
          )
          vertexData.set(tmpVertex.getValue.toInt, outerDatas(i))
          i += 1
        }
      }
      val time1 = System.nanoTime()
      log.info(
        s"[Perf: ] part ${pid} updating outer vertex data cost ${(time1 - time0) / 1000000}ms,  ovnum ${graphStructure.getOuterVertexSize}"
      )
    } else {
      log.info(
        s"[Perf]: part ${pid} receives no outer vertex data, startLid ${startLid}"
      )
    }
    this
  }

  def map[VD2: ClassTag](
      f: (VertexId, VD) => VD2
  ): GrapeVertexPartition[VD2] = {
    // Construct a view of the map transformation
    val time0     = System.nanoTime()
    val newValues = createNewValues[VD2]
    var i         = bitSet.nextSetBit(startLid)
    while (i >= 0 && i < endLid) {
      vertex.setValue(i)
      newValues.set(i, f(graphStructure.getId(vertex), getData(i)))
      i = bitSet.nextSetBit(i + 1)
    }

    val time1 = System.nanoTime()
    log.info(
      s"part ${pid} from ${startLid} to ${endLid} map vertex partition from ${GrapeUtils
        .getRuntimeClass[VD]
        .getSimpleName} to ${GrapeUtils.getRuntimeClass[VD2].getSimpleName}, active ${bitSet
        .cardinality()} cost ${(time1 - time0) / 1000000} ms"
    )
    this.withNewValues(newValues)
  }

  def filter(pred: (VertexId, VD) => Boolean): GrapeVertexPartition[VD] = {
    val newMask = new BitSetWithOffset(startLid, endLid)

    // Iterate over the active bits in the old mask and evaluate the predicate
    var curLid = bitSet.nextSetBit(startLid)
    while (curLid >= 0 && curLid < endLid) {
      vertex.setValue(curLid)
      if (pred(graphStructure.getId(vertex), getData(curLid))) {
        newMask.set(curLid)
      }
      curLid = bitSet.nextSetBit(curLid + 1)
    }
    this.withMask(newMask)
  }

  def withMask(newMask: BitSetWithOffset): GrapeVertexPartition[VD] = {
    new GrapeVertexPartition[VD](
      pid,
      startLid,
      endLid,
      localId,
      localNum,
      siblingPid,
      graphStructure,
      vertexData,
      client,
      routingTable,
      newMask
    )
  }

  def aggregateUsingIndex[VD2: ClassTag](
      iter: Iterator[Product2[VertexId, VD2]],
      reduceFunc: (VD2, VD2) => VD2
  ): GrapeVertexPartition[VD2] = {
    val newMask   = new BitSetWithOffset(startLid, endLid)
    val newValues = createNewValues[VD2]

    iter.foreach { product =>
      val oid   = product._1
      val vdata = product._2
      require(graphStructure.getVertex(oid, vertex))
      val lid = vertex.getValue().toInt
      if (lid >= 0) {
        if (newMask.get(lid)) {
          newValues.set(lid, reduceFunc(newValues.get(lid), vdata))
        } else { // otherwise just store the new value
          newMask.set(lid)
          newValues.set(lid, vdata)
        }
      }
    }
    this.withNewValues(newValues).withMask(newMask)
  }

  /** Hides the VertexId's that are the same between `this` and `other`. */
  def minus(other: GrapeVertexPartition[VD]): GrapeVertexPartition[VD] = {
    if (this.graphStructure != other.graphStructure) {
      logWarning(
        "Minus operations on two VertexPartitions with different indexes is slow."
      )
      minus(createUsingIndex(other.iterator))
    } else {
      this.withMask(this.bitSet.andNot(other.bitSet))
    }
  }

  def diff(other: GrapeVertexPartition[VD]): GrapeVertexPartition[VD] = {
    if (this.graphStructure != this.graphStructure) {
      logWarning("Diffing two VertexPartitions with different indexes is slow.")
      diff(createUsingIndex(other.iterator))
    } else {
      val newMask = this.bitSet & other.bitSet
      var i       = newMask.nextSetBit(startLid)
      while (i >= 0 && i < endLid) {
        if (getData(i) == other.getData(i)) {
          newMask.unset(i)
        }
        i = newMask.nextSetBit(i + 1)
      }
      this.withNewValues(other.vertexData).withMask(newMask)
    }
  }

  def leftJoin[VD2: ClassTag, VD3: ClassTag](
      other: GrapeVertexPartition[VD2]
  )(f: (VertexId, VD, Option[VD2]) => VD3): GrapeVertexPartition[VD3] = {
    if (this.graphStructure != other.graphStructure) {
      logWarning("Joining two VertexPartitions with different indexes is slow.")
      leftJoin(createUsingIndex(other.iterator))(f)
    } else {

      /** for vertex not represented in other, we use original vertex */
      val time0 = System.nanoTime()
      log.info(s"${GrapeUtils.getRuntimeClass[VD3].getSimpleName}")
      val newValues = createNewValues[VD3]
      var i         = this.bitSet.nextSetBit(startLid)
      while (i >= 0 && i < endLid) {
        vertex.setValue(i)
        val otherV: Option[VD2] =
          if (other.bitSet.get(i)) Some(other.getData(i)) else None
        val t = f(this.graphStructure.getId(vertex), this.getData(i), otherV)
        if (t == null) {
          log.info(s"when join ${this} and ${other} at pos ${i} result is null")
          throw new IllegalStateException("null......")
        }
        newValues.set(i, t)
        i = this.bitSet.nextSetBit(i + 1)
      }
      val time1 = System.nanoTime()
      if (localId == 0) {
        log.info(
          s"Left join between ${this} and ${other} cost ${(time1 - time0) / 1000000} ms"
        )
      }
      this.withNewValues(newValues)
    }
  }

  /** Similar effect as aggregateUsingIndex((a, b) => a)
    */
  def createUsingIndex[VD2: ClassTag](
      iter: Iterator[Product2[VertexId, VD2]]
  ): GrapeVertexPartition[VD2] = {
    val newMask   = new BitSetWithOffset(startLid, endLid)
    val newValues = createNewValues[VD2]
    iter.foreach { pair =>
      val vertexFound = graphStructure.getVertex(pair._1, vertex)
      if (vertexFound) {
        val lid = vertex.getValue().toInt
        newMask.set(lid)
        newValues.set(lid, pair._2)
      }
    }
    this.withNewValues(newValues).withMask(newMask)
  }

  /** Inner join another VertexPartition. */
  def innerJoin[U: ClassTag, VD2: ClassTag](
      other: GrapeVertexPartition[U]
  )(f: (VertexId, VD, U) => VD2): GrapeVertexPartition[VD2] = {
    if (this.graphStructure != other.graphStructure) {
      logWarning("Joining two VertexPartitions with different indexes is slow.")
      innerJoin(createUsingIndex(other.iterator))(f)
    } else {
      val newMask = this.bitSet & other.bitSet
      val newView = createNewValues[VD2]
      var i       = newMask.nextSetBit(startLid)
      while (i >= 0 && i < endLid) {
        vertex.setValue(i)
        newView.set(
          i,
          f(
            this.graphStructure.getId(vertex),
            this.getData(i),
            other.getData(i)
          )
        )
        i = newMask.nextSetBit(i + 1)
      }
      this.withNewValues(newView).withMask(newMask)
    }
  }

  /** create a new vertex partition with vertex updated according to the read vineyard id.
    * @param pathPrefix
    *   prefix of persisted vd path.
    * @return
    */
  def updateAfterPIE[ED: ClassTag](pathPrefix: String): GrapeVertexPartition[VD] = {
    if (localId == 0) {
      val path   = pathPrefix + graphStructure.fid();
      val reader = new BufferedReader(new FileReader(path))
      val line   = reader.readLine()
      require(line != null && line.nonEmpty, "read empty line")
      val fragId = line.toLong
      if (GrapeUtils.isPrimitive[VD]) {
        log.info(s"update after pid from frag id ${fragId}")
        val fragmentGetter = ScalaFFIFactory.newArrowProjectedFragmentGetter[VD, ED]
        val fragment       = fragmentGetter.get(client, fragId).get()

        val newVdatas = vertexData.mapToNew[VD]
        var i         = 0
        require(ivnum == fragment.getInnerVerticesNum, s"ivnum neq ${ivnum}, ${fragment.getInnerVerticesNum}")
        fragment match {
          case simple: ArrowProjectedFragment[Long, Long, VD, ED] => {
            if (GrapeUtils.isPrimitive[VD]) {
              val vdAccessor = ScalaFFIFactory.newPrimitiveTypedArray[VD]
              vdAccessor.setAddress(simple.getVdataArrayAccessor.getAddress)
              while (i < ivnum) {
                newVdatas.set(i, vdAccessor.get(i))
                i += 1
              }
            } else {
              val stringTypedArray: StringTypedArray = ScalaFFIFactory.newStringTypedArray
              stringTypedArray.setAddress(simple.getVdataArrayAccessor.getAddress)
              val vdAccessor =
                VertexDataUtils.readComplexArray[VD](stringTypedArray, GrapeUtils.getRuntimeClass[VD])
              while (i < ivnum) {
                newVdatas.set(i, vdAccessor(i))
                i += 1
              }
            }
          }
          case _ => throw new IllegalStateException("not possible")
        }

        log.info(s"pid ${pid} create immutable offHeap vertex data")
        for (dstPid <- siblingPid) {
          VertexDataStore.enqueue(dstPid, newVdatas)
        }
      } else {
        throw new IllegalStateException("Current not supported")
      }
    }
    val newStore =
      VertexDataStore.dequeue(pid).asInstanceOf[VertexDataStore[VD]]
    this.withNewValues(newStore)
  }

  def copyStringTypedArrayToHeap[T: ClassTag](
      typedArray: StringTypedArray
  ): Array[T] = {
    val vector =
      new FakeFFIByteVector(typedArray.getRawData, typedArray.getRawDataLength)
    val ffiInput = new FakeFFIByteVectorInputStream(vector)
    val len      = typedArray.getLength
    log.info(
      "reading {} objects from array of bytes {}",
      len,
      typedArray.getLength
    )
    val clz = GrapeUtils.getRuntimeClass[T]
    if (clz.equals(classOf[DoubleDouble])) {
      val newArray = new Array[DoubleDouble](len.toInt).asInstanceOf[Array[T]]
      var i        = 0
      while (i < len) {
        val a = ffiInput.readDouble
        val b = ffiInput.readDouble
        newArray(i) = new DoubleDouble(a, b).asInstanceOf[T]
        i += 1
      }
      newArray
    } else {
      val newArray          = new Array[T](len.toInt)
      val objectInputStream = new ObjectInputStream(ffiInput)
      var i                 = 0
      while (i < len) {
        val obj = objectInputStream.readObject.asInstanceOf[T]
        newArray(i) = obj
        i += 1
      }
      newArray
    }
  }

  def withNewValues[VD2: ClassTag](
      vds: VertexDataStore[VD2]
  ): GrapeVertexPartition[VD2] = {
    new GrapeVertexPartition[VD2](
      pid,
      startLid,
      endLid,
      localId,
      localNum,
      siblingPid,
      graphStructure,
      vds,
      client,
      routingTable,
      bitSet
    )
  }

  override def toString: String =
    "GrapeVertexPartition{" + "pid=" + pid + ",startLid=" + startLid + ", endLid=" + endLid + ",active=" + bitSet
      .cardinality() + '}'

  override def index: PartitionID = pid
}

object GrapeVertexPartition extends Logging {
  val pid2VertexStore: mutable.HashMap[Int, (Boolean, VertexDataStore[_])] =
    new mutable.HashMap[Int, (Boolean, VertexDataStore[_])]

  /** @param pid
    * @param initialized
    *   inidicate whether the pushed vertex data store has been filled with data array in shuffle
    * @param store
    */
  def setVertexStore(
      pid: Int,
      store: VertexDataStore[_],
      initialized: Boolean
  ): Unit = {
    require(!pid2VertexStore.contains(pid))
    pid2VertexStore(pid) = (initialized, store)
    log.info(s"storing part ${pid}'s inner vd store ${store.toString}'")
  }

  def fromEdgePartition[VD: ClassTag](
      value: VD,
      pid: Int,
      startLid: Long,
      endLid: Long,
      localId: Int,
      localNum: Int,
      siblingPids: Array[Int],
      client: VineyardClient,
      graphStructure: GraphStructure,
      routingTable: RoutingTable
  ): GrapeVertexPartition[VD] = {
    //copy to heap
    val (initialized, vertexStore) = (
      pid2VertexStore(pid)._1,
      pid2VertexStore(pid)._2.asInstanceOf[AbstractVertexDataStore[VD]]
    )
    if (!initialized) {
      if (localId == 0) {
        log.info(
          s"As vertex data is not initialized, setting value to default vd ${value}"
        )
      }
      var i     = startLid.toInt
      val limit = endLid
      while (i < limit) {
        vertexStore.set(i, value)
        i += 1
      }
    } else {
      if (localId == 0) {
        log.info("No need for initializing vertex attr")
      }
    }

    new GrapeVertexPartition[VD](
      pid,
      startLid.toInt,
      endLid.toInt,
      localId,
      localNum,
      siblingPids,
      graphStructure,
      vertexStore,
      client,
      routingTable
    )
  }
}
