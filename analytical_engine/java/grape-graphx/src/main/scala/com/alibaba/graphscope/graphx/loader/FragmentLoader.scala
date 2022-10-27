package com.alibaba.graphscope.graphx.loader

import com.alibaba.fastffi.{FFIByteString, FFITypeFactory}
import com.alibaba.graphscope.fragment.ArrowProjectedFragment
import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.rdd.{FragmentRDD, LocationAwareRDD}
import com.alibaba.graphscope.graphx.shuffle.DataShuffleHolder
import com.alibaba.graphscope.graphx.store.RawGraphData
import com.alibaba.graphscope.graphx.utils.{ExecutorUtils, GrapeUtils, ScalaFFIFactory}
import com.alibaba.graphscope.utils.MPIUtils
import org.apache.spark.SparkContext
import org.apache.spark.graphx.grape.GrapeEdgeRDD.gatherShufflePidArray
import org.apache.spark.graphx.grape.{GrapeEdgeRDD, GrapeGraphImpl, GrapeVertexRDD}
import org.apache.spark.graphx.scheduler.cluster.ExecutorInfoHelper
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.GSSparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
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
      hosts: Array[String],
      hostName2ExecutorId: mutable.HashMap[String, String],
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
    val rddHosts     = new Array[String](fragNum)
    val rddLocations = new Array[String](fragNum)
    val partitionIds = new Array[Int](fragNum)
    for (i <- 0 until fragNum) {
      rddHosts(i) = hosts(i)
      rddLocations(i) = "executor_" + hosts(i) + "_" + hostName2ExecutorId(hosts(i))
      partitionIds(i) = i
    }

    log.debug(s"rdd hosts ${rddHosts.mkString(",")}")
    log.debug(s"rdd locations ${rddLocations.mkString(",")}")
    log.debug(s"rdd partitions id ${partitionIds.mkString(",")}")
    val emptyRDD =
      new LocationAwareRDD(sc, rddLocations, rddHosts, partitionIds)

    val graphRDD = emptyRDD2Graph[VD, ED](
      sc,
      numShuffles,
      userNumPartitions,
      emptyRDD
    )
    prevRDD.unpersist()
    graphRDD
  }

  def emptyRDD2Graph[VD: ClassTag, ED: ClassTag](
      sc: SparkContext,
      numShuffles: Int,
      userNumPartitions: Int,
      emptyRDD: LocationAwareRDD
  ): GrapeGraphImpl[VD, ED] = {
    val numFrag = emptyRDD.getNumPartitions
    //For every executor, we get all partitions in this executor.
    val sparkSession = GSSparkSession.getDefaultSession.getOrElse(
      throw new IllegalStateException("empty session")
    )
    val socketPath = sparkSession.socketPath
    val shufflesRDD = emptyRDD
      .mapPartitionsWithIndex(
        (pid, iter) => {
          if (iter.hasNext) {
            val receivedShuffles = DataShuffleHolder.getArray
              .asInstanceOf[Array[DataShuffleHolder[VD, ED]]]
            Iterator((pid, receivedShuffles))
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
            val rawData = new RawGraphData[VD, ED](
              pid,
              numFrag,
              client,
              ExecutorUtils.getHostName,
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
      session.socketPath,
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
      session.socketPath
    )
    log.info(s"Constructed FragmentRDD ${FragmentRDD}")
    fragmentRDD
  }
}
