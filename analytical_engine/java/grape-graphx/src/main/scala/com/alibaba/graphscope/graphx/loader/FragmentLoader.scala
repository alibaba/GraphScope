package com.alibaba.graphscope.graphx.loader

import com.alibaba.fastffi.{FFIByteString, FFITypeFactory}
import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.rdd.{FragmentRDD, LocationAwareRDD}
import com.alibaba.graphscope.graphx.shuffle.DataShuffleHolder
import com.alibaba.graphscope.graphx.store.RawGraphData
import com.alibaba.graphscope.graphx.utils.{ExecutorUtils, GrapeUtils, ScalaFFIFactory}
import com.alibaba.graphscope.utils.MPIUtils
import org.apache.spark.SparkContext
import org.apache.spark.graphx.grape.GrapeGraphImpl
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.cluster.ExecutorInfoHelper
import org.apache.spark.sql.GSSparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.{ClassTag, classTag}

object FragmentLoader extends Logging {

  /** default vd == null means we will use vertex attr is shuffles.
    * @return
    */
  def lineRDD2Graph[VD: ClassTag, ED: ClassTag](
      sc: SparkContext,
      numShuffles: Int,
      userNumPartitions: Int,
      prevRDD: RDD[DataShuffleHolder[VD, ED]],
      fragNum: Int,
      hosts2Pids: mutable.HashMap[String, ArrayBuffer[Int]],
      hostName2ExecutorId: mutable.HashMap[String, ArrayBuffer[String]],
      vertexStorageLevel: StorageLevel,
      edgeStorageLevel: StorageLevel
  ): GrapeGraphImpl[VD, ED] = {
    prevRDD.foreachPartition(iter => {
      while (iter.hasNext) {
        val part = iter.next()
        DataShuffleHolder.push(part)
      }
    })
    //generate host,location,and partitionIds.
    //For each host, there maybe multiple executors, and can be multiple partitions. One partition
    //means one(at least one) ShuffleHolder in one executor.
    //From shuffle, we can only get the hostname info, but not the executor info(id).
    //Host h1, can have executor 0,3,5, one rdd has partition in 0,5, but not in 5.
    //to construct the empty rdd, we need to enumerate all executors, and eliminate empty partition.
    val executorId2Cores   = ExecutorInfoHelper.getExecutorsCores(sc)
    val hostname2Cores     = new mutable.HashMap[String, Int]()
    val hostArray          = hosts2Pids.keys.toArray
    val hostNum            = hostArray.length
    val numEmptyPartitions = hostName2ExecutorId.values.map(_.size).sum
    val rddHosts           = new Array[String](fragNum)
    val rddLocations       = new Array[String](fragNum)
    val partitionIds       = new Array[Int](fragNum)
    var hostInd            = 0
    var curPartition       = 0
    log.info(s"num Empty partitions ${numEmptyPartitions}, num Frag ${fragNum}")
    //one executor, one fragment,
    //the data must exist on there executors.
    for (host <- hostArray) {
      var curCores = 0
      for (executorId <- hostName2ExecutorId(host)) {
        curCores += executorId2Cores(executorId)
      }
      hostname2Cores(host) = curCores
      hostname2Cores(host) = hostname2Cores(host) / hostName2ExecutorId(host).size
      log.info(s"for host ${host}, there are executor ids ${hostName2ExecutorId(host)
        .mkString(",")} total cores ${curCores}, per executor parallelsim ${hostname2Cores(host)}")
    }

    while (hostInd < hostNum && curPartition < numEmptyPartitions) {
      val host = hostArray(hostInd)
      //the number of executor on one host can be more than the num partitions on this host. We take
      //the minimum value.
      val numExecutorOnThisHost = hostName2ExecutorId(host).size
      log.info(
        s"For host ${host}, we max create ${numExecutorOnThisHost} frag, executorNum on this host ${hostName2ExecutorId(
          host
        ).size}, part on this host ${hosts2Pids(host).size}"
      )
      val innerIterator = hostName2ExecutorId(host).iterator
      while (innerIterator.hasNext) {
        rddHosts(curPartition) = host
        rddLocations(curPartition) = "executor_" + host + "_" + innerIterator.next()
        partitionIds(curPartition) = curPartition
        curPartition += 1
      }
      hostInd += 1
    }
    require(
      hostInd == hostNum && curPartition == numEmptyPartitions,
      s"check equal failed ${hostInd} ?= ${hostNum}, ${curPartition} ?= ${numEmptyPartitions}"
    )

    log.info(s"rdd hosts ${rddHosts.mkString(",")}")
    log.info(s"rdd locations ${rddLocations.mkString(",")}")
    log.info(s"rdd partitions id ${partitionIds.mkString(",")}")
    val emptyRDD =
      new LocationAwareRDD(sc, rddLocations, rddHosts, partitionIds)
    //At this time, the partition num of this rdd is possibly larger than numFrag.

    val graphRDD = emptyRDD2Graph[VD, ED](
      sc,
      numShuffles,
      userNumPartitions,
      hostname2Cores,
      emptyRDD
    )
    prevRDD.unpersist()
    graphRDD
  }

  def emptyRDD2Graph[VD: ClassTag, ED: ClassTag](
      sc: SparkContext,
      numShuffles: Int,
      userNumPartitions: Int,
      parallelisms: mutable.HashMap[String, Int],
      emptyRDD: LocationAwareRDD
  ): GrapeGraphImpl[VD, ED] = {
    val numFrag = emptyRDD.getNumPartitions
    //For every executor, we get all partitions in this executor.
    val sparkSession = GSSparkSession.getDefaultSession.getOrElse(
      throw new IllegalStateException("empty session")
    )
    val socketPath = sparkSession.getSocketPath
    val shufflesRDD = emptyRDD
      .mapPartitionsWithIndex(
        (pid, iter) => {
          if (iter.hasNext) {
            val receivedShuffles = DataShuffleHolder.popAll
              .asInstanceOf[Array[DataShuffleHolder[VD, ED]]]
            if (receivedShuffles != null) {
              Iterator((pid, receivedShuffles))
            } else {
              log.info("No datashuffle holder found")
              Iterator.empty
            }
          } else {
            log.info(s"part ${pid} is empty")
            Iterator.empty
          }
        },
        preservesPartitioning = true
      )
      .cache()

    val metaRDD = shufflesRDD
      .mapPartitions(
        iter => {
          if (iter.hasNext) {
            val (pid, receivedShuffles) = iter.next()
            val client: VineyardClient = {
              val res                          = ScalaFFIFactory.newVineyardClient()
              val ffiByteString: FFIByteString = FFITypeFactory.newByteString()
              ffiByteString.copyFrom(socketPath)
              require(res.connect(ffiByteString).ok())
              log.info(s"successfully connect to ${socketPath}")
              res
            }
            val hostName = ExecutorUtils.getHostName
            val rawData = new RawGraphData[VD, ED](
              pid,
              numFrag,
              client,
              hostName,
              parallelisms(hostName),
              receivedShuffles
            )
            //expect size > 1
            log.info(
              s"partition ${pid} receive shuffles size ${receivedShuffles.size}"
            )
            Iterator((pid, rawData))
          } else Iterator.empty
        },
        preservesPartitioning = true
      )
      .cache()

    //for each metaRDD partition, construct temp data store, and return its vineyard id, launch mpi processes, return the arrowFragmentId.
    val collectedRawDataIds = metaRDD
      .mapPartitions(iter => {
        if (iter.hasNext) {
          val (pid, rawData) = iter.next()
          Iterator(
            ExecutorUtils.getHostName + ":" + rawData.partitionID + ":" + rawData.rawData
              .id()
          )
        } else Iterator.empty
      })
      .collect()
    log.info(
      s"[GrapeEdgeRDD]: Collected rawDataIds ${collectedRawDataIds.mkString(",")}"
    )
    log.info("[GrapeEdgeRDD]: Start constructing fragment")
    val fragmentRDD: FragmentRDD[VD, ED] =
      loadFragmentRDD(collectedRawDataIds, metaRDD)

    val (vertexRDD, edgeRDD) = fragmentRDD.generateRDD(userNumPartitions)
    GrapeGraphImpl.fromExistingRDDs[VD, ED](vertexRDD, edgeRDD)
  }

  /** Load fragment from received raw data ids.
    * @param rawDataIds
    *   in format "hostname:pid:objId,hostname:pid:objId"
    * @param shuffles
    * @tparam VD
    * @tparam ED
    * @return
    */
  def loadFragmentRDD[VD: ClassTag, ED: ClassTag](
      rawDataIds: Array[String],
      shuffles: RDD[(Int, RawGraphData[VD, ED])]
  ): FragmentRDD[VD, ED] = {
    val session = GSSparkSession.getDefaultSession
      .getOrElse(throw new IllegalStateException("Empty session"))
    val vdClass: Class[VD] =
      classTag[VD].runtimeClass.asInstanceOf[java.lang.Class[VD]]
    val edClass: Class[ED] =
      classTag[ED].runtimeClass.asInstanceOf[java.lang.Class[ED]]
    val fragIds = MPIUtils.loadFragment(
      rawDataIds,
      session.getSocketPath,
      vdClass,
      edClass
    )

    val sc = SparkContext.getOrCreate()
    val fragmentRDD = new FragmentRDD[VD, ED](
      sc,
      ExecutorInfoHelper.getExecutors(sc),
      "gs::ArrowProjectedFragment<int64_t,uint64_t," + GrapeUtils
        .classToStr[VD](true) + "," + GrapeUtils.classToStr[ED](true) + ">",
      fragIds.mkString(","),
      session.getSocketPath
    )
    log.info(s"Constructed FragmentRDD ${FragmentRDD}")
    fragmentRDD
  }
}
