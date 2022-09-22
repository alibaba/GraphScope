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

package org.apache.spark.graphx.grape

import com.alibaba.fastffi.{FFIByteString, FFITypeFactory}
import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.graph.impl.GraphXGraphStructure
import com.alibaba.graphscope.graphx.rdd.impl.{GrapeEdgePartition, GrapeEdgePartitionBuilder}
import com.alibaba.graphscope.graphx.rdd.{LocationAwareRDD, RoutingTable}
import com.alibaba.graphscope.graphx.shuffle.{EdgeShuffleReceived, EdgeShuffle}
import com.alibaba.graphscope.graphx.store.impl.InHeapVertexDataStore
import com.alibaba.graphscope.graphx.utils._
import com.alibaba.graphscope.utils.MPIUtils
import org.apache.spark.graphx.grape.impl.GrapeEdgeRDDImpl
import org.apache.spark.graphx.scheduler.cluster.ExecutorInfoHelper
import org.apache.spark.graphx.{Edge, EdgeRDD, PartitionID, VertexId}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{GSSparkSession, SparkSession}
import org.apache.spark.{Dependency, SparkContext}

import java.net.InetAddress
import scala.collection.mutable
import scala.reflect.ClassTag

abstract class GrapeEdgeRDD[ED](sc: SparkContext,
                                deps: Seq[Dependency[_]]) extends EdgeRDD[ED](sc, deps) {

  def grapePartitionsRDD: RDD[GrapeEdgePartition[VD, ED]] forSome { type VD }

  override def partitionsRDD = null

  def mapValues[ED2 : ClassTag](f: Edge[ED] => ED2): GrapeEdgeRDD[ED2]

  override def innerJoin[ED2: ClassTag, ED3: ClassTag](other: EdgeRDD[ED2])
  (f: (VertexId, VertexId, ED, ED2) => ED3): GrapeEdgeRDD[ED3]

  override def reverse: GrapeEdgeRDD[ED];
}

object GrapeEdgeRDD extends Logging{
  def fromPartitions[VD : ClassTag,ED : ClassTag](edgePartitions : RDD[GrapeEdgePartition[VD,ED]]) : GrapeEdgeRDDImpl[VD,ED] = {
    new GrapeEdgeRDDImpl[VD,ED](edgePartitions)
  }

  //(dstPid, (myPid,received gids(inner vertices)))
  def edgeRDDToRoutingTable[ED : ClassTag](rdd : GrapeEdgeRDD[ED]) : RDD[RoutingTable] = {
    val numPartitions = rdd.getNumPartitions
    val partitioner = new GSPartitioner[Int](numPartitions)
    val routingMessage = rdd.grapePartitionsRDD.mapPartitions(iter => {
      if (iter.hasNext){
        val part = iter.next()
        val messageArr = part.generateRoutingMessage()
        messageArr.zipWithIndex.map(a => (a._2,(part.pid, a._1))).toIterator
      }
      else Iterator.empty
    }).partitionBy(partitioner)
    val routingTableRDD = PartitionAwareZippedBaseRDD.zipPartitions(SparkContext.getOrCreate(), rdd.grapePartitionsRDD, routingMessage)((iter1, iter2) => {
      if (iter1.hasNext){
        val epart = iter1.next()
        //for routing messages send to me, construct routing table
        val routingTable = RoutingTable.fromMessage(epart.pid, numPartitions, epart.startLid.toInt, epart.endLid.toInt, iter2)
        Iterator(routingTable)
      }
      else Iterator.empty
    })
    routingTableRDD.cache()
  }

  def expand(globalVMIds: java.util.List[String], executorInfo: mutable.HashMap[String,String], targetLength : Int) : (Array[String],Array[String],Array[Int]) = {
    val size = globalVMIds.size()
    val hostNames = new Array[String](targetLength)
    val locations = new Array[String](targetLength)
    val partitionIds = new Array[Int](targetLength)
    for (i <- 0 until size){
      val splited = globalVMIds.get(i).split(":")
      require(splited.length == 3)
      hostNames(i) = splited(0)
      locations(i) = "executor_" + hostNames(i) + "_" + executorInfo(hostNames(i))
      partitionIds(i) = splited(1).toInt
    }

    var i = size
    while (i < targetLength){
      hostNames(i) = hostNames(i % size)
      locations(i) = locations(i % size)
      partitionIds(i) = i
      i += 1
    }
    log.info(s"expanded partitions,host names ${hostNames.mkString("Array(", ", ", ")")}")
    log.info(s"expanded partitions,locations ${locations.mkString("Array(", ", ", ")")}")
    log.info(s"expanded partitions,partitionIds ${partitionIds.mkString("Array(", ", ", ")")}")
    (hostNames, locations, partitionIds)
  }

  def sortLocalVertexMapIds(localVMIds : Array[String]) : Array[String] = {
    val pids = localVMIds.map(str => str.split(":")(1))
    val (pidsSorted, indices) = pids.zipWithIndex.sorted.unzip
    val res = new Array[String](localVMIds.length)
    var i = 0
    while (i < pids.length){
      res(i) = localVMIds(indices(i))
      i += 1
    }
    res
  }

  def gatherShufflePidArray[ED : ClassTag](numShuffles : Int, shuffleRDD: RDD[(Int, Array[EdgeShuffleReceived[ED]])]): Array[Int] = {
    val pidsAndShuffleIds = shuffleRDD.mapPartitionsWithIndex((pid,iter) => {
      if (iter.hasNext){
        val (_pid, part) = iter.next()
        require(_pid == pid, s"neq ${_pid} ${pid}")
        val bitset = new mutable.BitSet()
        for (received <- part){
          for (shuffle <- received.fromPid2Shuffle){
            bitset.add(shuffle.dstPid)
          }
        }
        val array = bitset.toArray
        log.info(s"empty rdd part ${pid} got shuffles dst ids ${array.mkString(",")}")
        Iterator((pid, array))
      }
      else {
        log.info(s"part ${pid} in gather shuffle pid array is empty")
        Iterator.empty
      }
    }).collect()
    val resArray = Array.fill(numShuffles)(-1)
    for (arr <- pidsAndShuffleIds){
      val shuffleIds = arr._2
      val graphxPid = arr._1
      for (shuffleId <- shuffleIds){
        if (resArray(shuffleId) != -1){
          throw new IllegalStateException(s"Not possible, try to set ${shuffleId} but already set with ${resArray(shuffleId)}")
        }
        resArray(shuffleId) = graphxPid
      }
    }
    for (i <- resArray.indices){
      require(resArray(i) != -1, s"index ${i} still -1 after set")
    }
    resArray
  }

  def fromEmptyRDD[VD : ClassTag,ED : ClassTag](sc : SparkContext, numShuffles : Int, userNumPartitions : Int, emptyRDD : LocationAwareRDD, defaultED : ED = null.asInstanceOf[ED]) : GrapeEdgeRDD[ED] = {

    val numFrag = emptyRDD.getNumPartitions
    //For every executor, we get all partitions in this executor.
    val sparkSession = GSSparkSession.getDefaultSession.getOrElse(throw new IllegalStateException("empty session")).asInstanceOf[GSSparkSession]
    val socketPath = sparkSession.socketPath
    val shufflesRDD = emptyRDD.mapPartitionsWithIndex((pid,iter) => {
      if (iter.hasNext){
        val receivedShuffles = EdgeShuffleReceived.getArray.asInstanceOf[Array[EdgeShuffleReceived[ED]]]
        Iterator((pid,receivedShuffles))
      }
      else {
        log.info(s"part ${pid} is empty")
        Iterator.empty
      }
    },preservesPartitioning = true).cache()

    val shufflePid2Array = gatherShufflePidArray(numShuffles,shufflesRDD)
    log.info(s"shuffle index to graphx pid mapping ${shufflePid2Array.mkString(",")}")

    val metaRDD = shufflesRDD.mapPartitions(iter => {
      if (iter.hasNext) {
        val (pid,receivedShuffles) = iter.next()
        val client: VineyardClient = {
          val res = ScalaFFIFactory.newVineyardClient()
          val ffiByteString: FFIByteString = FFITypeFactory.newByteString()
          ffiByteString.copyFrom(socketPath)
          require(res.connect(ffiByteString).ok())
          log.info(s"successfully connect to ${socketPath}")
          res
        }
        val grapeMeta = new GrapeMeta[VD, ED](pid, numFrag, client, ExecutorUtils.getHostName)
        val edgePartitionBuilder = new GrapeEdgePartitionBuilder[VD, ED](numFrag, client)
        //expect size > 1
        log.info(s"partition ${pid} receive shuffles size ${receivedShuffles.size}")
        edgePartitionBuilder.addEdges(receivedShuffles)
        grapeMeta.setEdgePartitionBuilder(edgePartitionBuilder)
        val parallelism = Runtime.getRuntime.availableProcessors();

        val localVertexMap = edgePartitionBuilder.buildLocalVertexMap(pid, parallelism,shufflePid2Array, numShuffles)
        if (localVertexMap == null) {
          Iterator.empty
        }
        else {
          grapeMeta.setLocalVertexMap(localVertexMap)
          grapeMeta.setEdgePartitionBuilder(edgePartitionBuilder)
          Iterator(grapeMeta)
        }
      }
      else Iterator.empty
    },preservesPartitioning = true).cache()


    val localVertexMapIds = metaRDD.mapPartitions(iter => {
      if (iter.hasNext){
        val meta = iter.next()
        Iterator(ExecutorUtils.getHostName + ":" + meta.partitionID + ":" + meta.localVertexMap.id())
      }
      else Iterator.empty
    },preservesPartitioning = true).collect().distinct.sorted

    log.info(s"[GrapeEdgeRDD]: got distinct local vm ids ${localVertexMapIds.mkString("Array(", ", ", ")")}")
    require(localVertexMapIds.length == numFrag, s"${localVertexMapIds.length} neq to num partitoins ${numFrag}")
    val sortedLocalVMIds = sortLocalVertexMapIds(localVertexMapIds)
    //pid should match with fid.

    log.info("[GrapeEdgeRDD]: Start constructing global vm")
    val time0 = System.nanoTime()
    val globalVMIDs = MPIUtils.constructGlobalVM(sortedLocalVMIds,socketPath  , "int64_t", "uint64_t")
    log.info(s"[GrapeEdgeRDD]: Finish constructing global vm ${globalVMIDs}, cost ${(System.nanoTime() - time0)/1000000} ms")
    require(globalVMIDs.size() == numFrag)
    log.info(s"meta partition size ${metaRDD.count}")

    val metaPartitionsUpdated = metaRDD.mapPartitions(iter => {
      if (iter.hasNext) {
        val meta = iter.next()
        val hostName = InetAddress.getLocalHost.getHostName
        log.info(s"Doing meta update on ${}, pid ${meta.partitionID}")
        var res = null.asInstanceOf[String]
        var i = 0
        while (i < globalVMIDs.size()) {
          val ind = globalVMIDs.get(i).indexOf(hostName)
          if (ind != -1) {
            val spltted = globalVMIDs.get(i).split(":")
            require(spltted.length == 3) // hostname,pid,vmid
            require(spltted(0).equals(hostName))
            if (spltted(1).toInt == meta.partitionID) {
              res = spltted(2)
            }
          }
          i += 1
        }
        require(res != null, s"after iterate over received global ids, no suitable found for ${hostName}, ${meta.partitionID} : ${globalVMIDs}")
        meta.setGlobalVM(res.toLong)
        val (vm, csr) = meta.edgePartitionBuilder.buildCSR(meta.globalVMId)
        val time0 = System.nanoTime()
        //We will build a store which underlying is a simple array with length csr.getTotalEdgesNum,
        //but we can get out edge data from it with oeoffset, with some what conversion.
        val edatas = meta.edgePartitionBuilder.buildEdataStore(defaultED, csr.getTotalEdgesNum.toInt,meta.vineyardClient, new EIDAccessor(csr.getOEBegin(0).getAddress))
        val time1 = System.nanoTime()
        log.info(s"Create edata cost ${(time1 - time0)/1000000}ms")
        //raw edatas contains all edge datas, i.e. csr edata array.
        //edatas are out edges edge cache.
        meta.setGlobalVM(vm)
        meta.setCSR(csr)
        meta.setEdataStore(edatas)
        Iterator(meta)
      }
      else Iterator.empty
    },preservesPartitioning = true).cache()

    metaPartitionsUpdated.foreachPartition(iter => {
      log.info("doing edge partition building")
      if (iter.hasNext) {
        val meta = iter.next()
        val time0 = System.nanoTime()
        val vm = meta.globalVM
        //set numSplit later
        val vertexDataStore = new InHeapVertexDataStore[VD](vm.getVertexSize.toInt, 1, meta.vineyardClient)
        val edgeBuilder = meta.edgePartitionBuilder
        val graphStructure = new GraphXGraphStructure(meta.globalVM, meta.graphxCSR)

        val initialized = edgeBuilder.fillVertexData(vertexDataStore, graphStructure)
        val time1 = System.nanoTime()
        log.info(s"[Creating graph structure cost ]: ${(time1 - time0) / 1000000} ms")
        GrapeEdgePartition.push((meta.partitionID,graphStructure, meta.vineyardClient,meta.edataStore,vertexDataStore, initialized))

        meta.edgePartitionBuilder.clearBuilders()
        meta.edgePartitionBuilder = null //make it null to let it be gc able
      }
    })

    val executorInfo = ExecutorInfoHelper.getExecutorsHost2Id(sc)
    //meta updated only has fnum partitions, we need to create userNumPartitions partitions.
    log.info(s"edge shuffle num partition ${numFrag}, target num parts ${userNumPartitions}")
    val (expandedHosts, expandedLocations,expandedPartitionIds) = expand(globalVMIDs, executorInfo, userNumPartitions)
    require(expandedHosts.length == userNumPartitions)

    val res = createUserPartitionsAndFormRDD[VD,ED](sc, expandedHosts,expandedLocations,expandedPartitionIds,userNumPartitions)
    //clear cached builder memory
    metaPartitionsUpdated.unpersist()
    metaRDD.unpersist()
    metaRDD.unpersist()
    shufflesRDD.unpersist()
    res
  }

  /**
   *
   * This procedure is shared by fragment rdd and grape rdd. GrapeEdgePartition.push should be called
   * before this.
   */
  def createUserPartitionsAndFormRDD[VD: ClassTag,ED : ClassTag](sc: SparkContext, expandedHosts : Array[String], expandedLocations : Array[String], expandedPartitionIds : Array[Int], userNumPartitions : Int) : GrapeEdgeRDD[ED] = {
    require(userNumPartitions == expandedHosts.length, s"size neq ${userNumPartitions}, ${expandedHosts.length}")
    val grapeInitRDD = new LocationAwareRDD(sc, expandedLocations,expandedHosts,expandedPartitionIds)
    grapeInitRDD.foreachPartition(iter => {
      if (iter.hasNext){
        val part = iter.next()
        lazy val hostName = InetAddress.getLocalHost.getHostName
        require(hostName.equals(part.hostName), s"part ${part.ind} host name neq ${part.hostName}, ${InetAddress.getLocalHost.getHostName}")
        GrapeEdgePartition.incCount(part.ind)
      }
    })
    grapeInitRDD.foreachPartition(iter => {
      if (iter.hasNext){
        GrapeEdgePartition.createPartitions[VD,ED](iter.next().ind,userNumPartitions)
      }
    })
    log.info(s"empty rdd size ${grapeInitRDD.getNumPartitions}")

    val grapeEdgePartitions = grapeInitRDD.mapPartitionsWithIndex((pid, iter) => {
      if (iter.hasNext){
        val grapeEdgePartition = GrapeEdgePartition.get[VD,ED](pid)
        log.info(s"part ${pid} got grapeEdgePart ${grapeEdgePartition.toString}")
        Iterator(grapeEdgePartition)
      }
      else Iterator.empty
    },preservesPartitioning = true).cache()

    //collect partition mapping info
    val partitionInfo = grapeEdgePartitions.mapPartitions(iter => {
      if (iter.hasNext){
        val part = iter.next()
        Iterator((part.pid,part.graphStructure.fid(), part.startLid,part.endLid))
      }
      else Iterator.empty
    }).collect() // (graphx pid, fid, start lid, end lid)

    grapeEdgePartitions.foreachPartition(iter => {
      if (iter.hasNext){
        val part = iter.next()
        part.buildPartitionInfo(partitionInfo)
      }
    })

    val rdd = new GrapeEdgeRDDImpl[VD,ED](grapeEdgePartitions)
    log.info(s"[GrapeEdgeRDD:] Finish Construct EdgeRDD, total edges count ${rdd.count()}")
    rdd
  }
}
