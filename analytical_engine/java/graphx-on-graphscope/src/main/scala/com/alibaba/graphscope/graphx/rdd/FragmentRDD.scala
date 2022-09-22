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

package com.alibaba.graphscope.graphx.rdd

import com.alibaba.fastffi.{FFIByteString, FFITypeFactory}
import com.alibaba.graphscope.ds.Vertex
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor
import com.alibaba.graphscope.fragment.{ArrowProjectedFragment, IFragment}
import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.graph.impl.FragmentStructure
import com.alibaba.graphscope.graphx.rdd.FragmentPartition.getHost
import com.alibaba.graphscope.graphx.rdd.impl.GrapeEdgePartition
import com.alibaba.graphscope.graphx.store.impl.{ImmutableOffHeapEdgeStore, InHeapVertexDataStore}
import com.alibaba.graphscope.graphx.store.{EdgeDataStore, VertexDataStore}
import com.alibaba.graphscope.graphx.utils.{EIDAccessor, ScalaFFIFactory}
import com.alibaba.graphscope.utils.FFITypeFactoryhelper
import org.apache.spark.graphx.PartitionID
import org.apache.spark.graphx.grape.{GrapeEdgeRDD, GrapeVertexRDD}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partition, SparkContext, TaskContext}

import java.net.InetAddress
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.reflect.ClassTag

class FragmentPartition[VD : ClassTag,ED : ClassTag](rddId : Int, override val index : Int, val hostName : String,val executorId : String,objectID : Long, socket : String, fragName : String) extends Partition with Logging {

  /** mark this val as lazy to let it run on executor rather than driver */
  lazy val tuple = {
    if (hostName.equals(getHost)){
      val client: VineyardClient = ScalaFFIFactory.newVineyardClient()
      val ffiByteString: FFIByteString = FFITypeFactory.newByteString()
      ffiByteString.copyFrom(socket)
      client.connect(ffiByteString)
      log.info(s"Create vineyard client ${client} and connect to ${socket}")
      val fragment: IFragment[Long, Long, VD, ED] = ScalaFFIFactory.getFragment[VD,ED](client, objectID, fragName)
      log.info(s"Got iFragment ${fragment}")
      (client, fragment)
    }
    else {
      log.info(s"This partition should be evaluated on this host since it is not on the desired host,desired host ${hostName}, cur host ${getHost}")
      null
    }
  }

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)

}
object FragmentPartition{
  def getHost : String = {
    InetAddress.getLocalHost.getHostName
  }
}

class FragmentRDD[VD : ClassTag,ED : ClassTag](@transient sc : SparkContext, executorId2Host : mutable.HashMap[String,String],
                                               fragName: String, objectIDs : String, socket : String = "/tmp/vineyard.sock")  extends RDD[(PartitionID,(VineyardClient,IFragment[Long,Long,VD,ED]))](sc, Nil) with Logging{
  //objectIds be like d50:id1,d51:id2
  val objectsSplited: Array[String] = objectIDs.split(",")
  val map: mutable.Map[String,Long] = mutable.Map[String, Long]()
  require(objectsSplited.length == executorId2Host.size, s"executor's host names length not equal to object ids ${executorId2Host.toArray.mkString("Array(", ", ", ")")}, ${objectIDs}")
  for (str <- objectsSplited){
    val hostAndId = str.split(":")
    require(hostAndId.length == 2)
    val host = hostAndId(0)
    val id = hostAndId(1)
    require(!map.contains(host), s"entry for host ${host} already set ${map.get(host)}")
    map(host) = id.toLong
    log.info(s"host ${host}: objId : ${id}")
  }


  override def compute(split: Partition, context: TaskContext): Iterator[(PartitionID,(VineyardClient,IFragment[Long,Long,VD,ED]))] = {
    val partitionCasted = split.asInstanceOf[FragmentPartition[VD,ED]]
    Iterator((partitionCasted.index, partitionCasted.tuple))
  }

  /** according to spark code comments, this function will be only executed once. */
  override protected def getPartitions: Array[Partition] = {
    val array = new Array[Partition](objectsSplited.length)
    val iter = executorId2Host.iterator
    for (i <- array.indices) {
      val (executorId, executorHost) = iter.next()
      log.info(s"executorId ${executorId}, host ${executorHost}, corresponding obj id ${map(executorHost)}")
      array(i) = new FragmentPartition[VD,ED](id, i, executorHost, executorId, map(executorHost), socket,fragName)
    }
    array
  }

  /**
   * Use this to control the location of partition.
   * @param split
   * @return
   */
  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val casted = split.asInstanceOf[FragmentPartition[VD,ED]]
    //TaskLocation.executorTag = executor_
    val location = "executor_" + casted.hostName + "_" + casted.executorId
    log.info(s"get pref location for ${casted.hostName} ${location}")
    Array(location)
  }

  def generateEdgeRDD(userNumPartitions: Int) : GrapeEdgeRDD[ED] = {
    val structAndVEStorePartitions = this.mapPartitions(iter => {
      if (iter.hasNext){
        val (pid, (client,frag)) = iter.next()
        val time0 = System.nanoTime()
        val structure = new FragmentStructure(frag)
        //update split num later
        val vertexDataStore = new InHeapVertexDataStore[VD](frag.getVerticesNum.toInt, 1,client)
        val parallelism = Runtime.getRuntime.availableProcessors();
        //fill vertex data store, copy.
        FragmentRDD.fillVertexStore(vertexDataStore, frag, parallelism)
        //edge is large,no copy
        val edgeStore = FragmentRDD.createFragmentEdataStore(frag,parallelism,client)
        //fill edge data store
        val time1 = System.nanoTime()
        log.info(s"[Creating graph structure and vd,ed store cost ]: ${(time1 - time0) / 1000000} ms")
        Iterator((pid, (client, frag, structure, vertexDataStore, edgeStore)))
      }
      else Iterator.empty
    }).cache()
    structAndVEStorePartitions.foreachPartition(iter => {
      if (iter.hasNext) {
        val (pid,(client,frag, structure, vertexStore, edgeStore)) = iter.next()
        GrapeEdgePartition.push((pid,structure, client,edgeStore,vertexStore, true))
      }
    })

    val partitions = getPartitions
    val (expandedHosts, expandedLocations, expendedPartitionIds) = expand(userNumPartitions, partitions.map(_.asInstanceOf[FragmentPartition[VD,ED]]))
    GrapeEdgeRDD.createUserPartitionsAndFormRDD[VD,ED](sc,expandedHosts,expandedLocations,expendedPartitionIds,userNumPartitions)
  }

  def expand(userNumPartitions : Int, partitions : Array[FragmentPartition[VD,ED]]) : (Array[String],Array[String],Array[Int]) = {
    val curSize = partitions.length
    log.info(s"expand ${curSize} frags to ${userNumPartitions} partitions")
    val hostsArray = new Array[String](userNumPartitions)
    val locationsArray = new Array[String](userNumPartitions)
    val partitionsIds = new Array[Int](userNumPartitions)

    for (i <- partitions.indices){
      hostsArray(i) = partitions(i).hostName
      locationsArray(i) = "executor_" + partitions(i).hostName + "_" + partitions(i).executorId
      partitionsIds(i) = i
    }
    for (i <- curSize until userNumPartitions){
      hostsArray(i) = hostsArray(i % curSize)
      locationsArray(i) = locationsArray(i % curSize)
      partitionsIds(i) = i
    }
    log.info(s"expanded partitions,host names ${hostsArray.mkString("Array(", ", ", ")")}")
    log.info(s"expanded partitions,locations ${locationsArray.mkString("Array(", ", ", ")")}")
    log.info(s"expanded partitions,partitionIds ${partitionsIds.mkString("Array(", ", ", ")")}")
    (hostsArray, locationsArray,partitionsIds)
  }

  def generateRDD(userNumPartitions : Int, vertexStorageLevel : StorageLevel = StorageLevel.MEMORY_ONLY) : (GrapeVertexRDD[VD],GrapeEdgeRDD[ED]) = {
    require(userNumPartitions >= getNumPartitions, s"can not construct ${userNumPartitions} partitions from ${getNumPartitions} frag")
    this.cache()
    val time0 = System.nanoTime()
    val edgeRDD = generateEdgeRDD(userNumPartitions)

    val routingTableRDD = GrapeEdgeRDD.edgeRDDToRoutingTable(edgeRDD)

    val vertexRDD = GrapeVertexRDD.fromGrapeEdgeRDD[VD](edgeRDD, routingTableRDD, edgeRDD.grapePartitionsRDD.getNumPartitions, null.asInstanceOf[VD],vertexStorageLevel).cache()
    log.info(s"num vertices ${vertexRDD.count()}, num edges ${edgeRDD.count()}")
    val time1 = System.nanoTime()
    log.info(s"[FragmentRDD:] generate rdd of partition ${userNumPartitions} cost ${(time1 - time0)/1000000}ms")
    (vertexRDD,edgeRDD)
  }
}
object FragmentRDD{

  def fillVertexStore[VD: ClassTag](vertexDataStore: VertexDataStore[VD],frag: IFragment[Long,Long,VD,_], numCores : Int) : Unit = {
    val chunkSize = 8192
    val atomicInt = new AtomicInteger(0)
    val threads = new Array[Thread](numCores)
    var tid = 0
    val ivnum = frag.getInnerVerticesNum
    while (tid < numCores) {
      val threadId= tid
      val newThread = new Thread() {
        override def run(): Unit = {
          var flag = true
          val vertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
          while (flag) {
            val got = atomicInt.getAndAdd(1);
            val begin = Math.min(ivnum, got * chunkSize)
            val end = Math.min(ivnum, begin + chunkSize)
            if (begin >= end) {
              flag = false
            }
            else {
              var i = begin
              while (i < end){
                vertex.SetValue(i)
                vertexDataStore.set(i.toInt, frag.getData(vertex))
                i += 1
              }
            }
          }
        }
      }
      newThread.start()
      threads(tid) = newThread
      tid += 1
    }
    for (i <- 0 until numCores) {
      threads(i).join()
    }
  }

  def createFragmentEdataStore[ED: ClassTag](frag : IFragment[Long,Long,_,ED], numCores : Int, client : VineyardClient) : EdgeDataStore[ED] = {
    frag match {
      case adaptor : ArrowProjectedAdaptor[Long,Long,_,ED] => {
        val projectedFragment = adaptor.getArrowProjectedFragment.asInstanceOf[ArrowProjectedFragment[Long,Long,_,ED]]
        val edataArray = projectedFragment.getEdataArrayAccessor
        val edataStore = new ImmutableOffHeapEdgeStore[ED](edataArray.getLength.toInt, 1, client, new EIDAccessor(projectedFragment.getOutEdgesPtr.getAddress), edataArray)
        edataStore
        //no need to copy
      }
      case _ => {
        throw new IllegalStateException(s"Not supported adaptor ${frag}")
      }
    }
  }
}
