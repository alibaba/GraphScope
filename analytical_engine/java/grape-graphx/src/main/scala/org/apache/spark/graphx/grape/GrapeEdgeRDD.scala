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

import com.alibaba.graphscope.graphx.rdd.impl.GrapeEdgePartition
import com.alibaba.graphscope.graphx.rdd.{LocationAwareRDD, RoutingTable}
import com.alibaba.graphscope.graphx.shuffle.DataShuffleHolder
import com.alibaba.graphscope.graphx.utils._
import org.apache.spark.graphx.grape.impl.GrapeEdgeRDDImpl
import org.apache.spark.graphx.{Edge, EdgeRDD, VertexId}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkContext}

import java.net.InetAddress
import scala.collection.mutable
import scala.reflect.ClassTag

abstract class GrapeEdgeRDD[ED](sc: SparkContext, deps: Seq[Dependency[_]]) extends EdgeRDD[ED](sc, deps) {

  def grapePartitionsRDD: RDD[GrapeEdgePartition[VD, ED]] forSome { type VD }

  override def partitionsRDD = null

  def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): GrapeEdgeRDD[ED2]

  override def innerJoin[ED2: ClassTag, ED3: ClassTag](other: EdgeRDD[ED2])(
      f: (VertexId, VertexId, ED, ED2) => ED3
  ): GrapeEdgeRDD[ED3]

  override def reverse: GrapeEdgeRDD[ED];
}

object GrapeEdgeRDD extends Logging {
  def fromPartitions[VD: ClassTag, ED: ClassTag](
      edgePartitions: RDD[GrapeEdgePartition[VD, ED]]
  ): GrapeEdgeRDDImpl[VD, ED] = {
    new GrapeEdgeRDDImpl[VD, ED](edgePartitions)
  }

  //(dstPid, (myPid,received gids(inner vertices)))
  def edgeRDDToRoutingTable[ED: ClassTag](
      rdd: GrapeEdgeRDD[ED]
  ): RDD[RoutingTable] = {
    val numPartitions = rdd.getNumPartitions
    val partitioner   = new GSPartitioner[Int](numPartitions)
    val routingMessage = rdd.grapePartitionsRDD
      .mapPartitions(iter => {
        if (iter.hasNext) {
          val part       = iter.next()
          val messageArr = part.generateRoutingMessage()
          messageArr.zipWithIndex.map(a => (a._2, (part.pid, a._1))).toIterator
        } else Iterator.empty
      })
      .partitionBy(partitioner)
    val routingTableRDD = PartitionAwareZippedBaseRDD.zipPartitions(
      SparkContext.getOrCreate(),
      rdd.grapePartitionsRDD,
      routingMessage
    )((iter1, iter2) => {
      if (iter1.hasNext) {
        val epart = iter1.next()
        //for routing messages send to me, construct routing table
        val routingTable = RoutingTable.fromMessage(
          epart.pid,
          numPartitions,
          epart.startLid.toInt,
          epart.endLid.toInt,
          iter2
        )
        Iterator(routingTable)
      } else Iterator.empty
    })
    routingTableRDD.cache()
  }

  def expand(
      globalVMIds: java.util.List[String],
      executorInfo: mutable.HashMap[String, String],
      targetLength: Int
  ): (Array[String], Array[String], Array[Int]) = {
    val size         = globalVMIds.size()
    val hostNames    = new Array[String](targetLength)
    val locations    = new Array[String](targetLength)
    val partitionIds = new Array[Int](targetLength)
    for (i <- 0 until size) {
      val splited = globalVMIds.get(i).split(":")
      require(splited.length == 3)
      hostNames(i) = splited(0)
      locations(i) = "executor_" + hostNames(i) + "_" + executorInfo(hostNames(i))
      partitionIds(i) = splited(1).toInt
    }

    var i = size
    while (i < targetLength) {
      hostNames(i) = hostNames(i % size)
      locations(i) = locations(i % size)
      partitionIds(i) = i
      i += 1
    }
    log.info(
      s"expanded partitions,host names ${hostNames.mkString("Array(", ", ", ")")}"
    )
    log.info(
      s"expanded partitions,locations ${locations.mkString("Array(", ", ", ")")}"
    )
    log.info(
      s"expanded partitions,partitionIds ${partitionIds.mkString("Array(", ", ", ")")}"
    )
    (hostNames, locations, partitionIds)
  }

  def gatherShufflePidArray[VD: ClassTag, ED: ClassTag](
      numShuffles: Int,
      shuffleRDD: RDD[(Int, Array[DataShuffleHolder[VD, ED]])]
  ): Array[Int] = {
    val pidsAndShuffleIds = shuffleRDD
      .mapPartitionsWithIndex((pid, iter) => {
        if (iter.hasNext) {
          val (_pid, part) = iter.next()
          require(_pid == pid, s"neq ${_pid} ${pid}")
          val bitset = new mutable.BitSet()
          for (received <- part) {
            for (shuffle <- received.fromPid2Shuffle) {
              bitset.add(shuffle.dstPid)
            }
          }
          val array = bitset.toArray
          log.info(
            s"empty rdd part ${pid} got shuffles dst ids ${array.mkString(",")}"
          )
          Iterator((pid, array))
        } else {
          log.info(s"part ${pid} in gather shuffle pid array is empty")
          Iterator.empty
        }
      })
      .collect()
    val resArray = Array.fill(numShuffles)(-1)
    for (arr <- pidsAndShuffleIds) {
      val shuffleIds = arr._2
      val graphxPid  = arr._1
      for (shuffleId <- shuffleIds) {
        if (resArray(shuffleId) != -1) {
          throw new IllegalStateException(
            s"Not possible, try to set ${shuffleId} but already set with ${resArray(shuffleId)}"
          )
        }
        resArray(shuffleId) = graphxPid
      }
    }
    for (i <- resArray.indices) {
      require(resArray(i) != -1, s"index ${i} still -1 after set")
    }
    resArray
  }

  /** This procedure is shared by fragment rdd and grape rdd. GrapeEdgePartition.push should be called before this.
    */
  def createUserPartitionsAndFormRDD[VD: ClassTag, ED: ClassTag](
      sc: SparkContext,
      expandedHosts: Array[String],
      expandedLocations: Array[String],
      expandedPartitionIds: Array[Int],
      userNumPartitions: Int
  ): GrapeEdgeRDD[ED] = {
    require(
      userNumPartitions == expandedHosts.length,
      s"size neq ${userNumPartitions}, ${expandedHosts.length}"
    )
    val grapeInitRDD = new LocationAwareRDD(
      sc,
      expandedLocations,
      expandedHosts,
      expandedPartitionIds
    )
    grapeInitRDD.foreachPartition(iter => {
      if (iter.hasNext) {
        val part          = iter.next()
        lazy val hostName = InetAddress.getLocalHost.getHostName
        require(
          hostName.equals(part.hostName),
          s"part ${part.ind} host name neq ${part.hostName}, ${InetAddress.getLocalHost.getHostName}"
        )
        GrapeEdgePartition.incCount(part.ind)
      }
    })
    grapeInitRDD.foreachPartition(iter => {
      if (iter.hasNext) {
        GrapeEdgePartition
          .createPartitions[VD, ED](iter.next().ind, userNumPartitions)
      }
    })
    log.info(s"empty rdd size ${grapeInitRDD.getNumPartitions}")

    val grapeEdgePartitions = grapeInitRDD
      .mapPartitionsWithIndex(
        (pid, iter) => {
          if (iter.hasNext) {
            val grapeEdgePartition = GrapeEdgePartition.get[VD, ED](pid)
            log.info(
              s"part ${pid} got grapeEdgePart ${grapeEdgePartition.toString}"
            )
            Iterator(grapeEdgePartition)
          } else Iterator.empty
        },
        preservesPartitioning = true
      )
      .cache()

    //collect partition mapping info
    val partitionInfo = grapeEdgePartitions
      .mapPartitions(iter => {
        if (iter.hasNext) {
          val part = iter.next()
          Iterator(
            (part.pid, part.graphStructure.fid(), part.startLid, part.endLid)
          )
        } else Iterator.empty
      })
      .collect() // (graphx pid, fid, start lid, end lid)

    grapeEdgePartitions.foreachPartition(iter => {
      if (iter.hasNext) {
        val part = iter.next()
        part.buildPartitionInfo(partitionInfo)
      }
    })

    val rdd = new GrapeEdgeRDDImpl[VD, ED](grapeEdgePartitions)
    log.info(
      s"[GrapeEdgeRDD:] Finish Construct EdgeRDD, total edges count ${rdd.count()}"
    )
    rdd
  }
}
