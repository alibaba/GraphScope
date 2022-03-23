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

import com.alibaba.graphscope.graphx.graph.GraphStructureTypes.GraphStructureType
import com.alibaba.graphscope.graphx.graph.impl.FragmentStructure
import com.alibaba.graphscope.graphx.store.VertexDataStore
import com.alibaba.graphscope.graphx.utils.{BitSetWithOffset, ExecutorUtils}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.grape.impl.{GrapeEdgeRDDImpl, GrapeVertexRDDImpl}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.{ClassTag, classTag}

object GrapeGraphBackend extends Enumeration {
  val ArrowProjectedFragment = GrapeGraphBackend
}

/** Creating a graph abstraction by combining vertex RDD and edge RDD together. Before doing this construction:
  *   - Both vertex RDD and edge RDD are available for map,fliter operators.
  *   - Both vertex RDD and edge RDD stores data in partitions When construct this graph, we will
  *   - copy data out to shared-memory
  *   - create mpi processes to load into fragment.
  *   - Wrap fragment as GrapeGraph when doing pregel computation. When changes made to graphx graph, it will not
  *     directly take effect on grape-graph. To apply these changes to grape-graph. Invoke to grape graph directly.
  *
  * @param vertices
  *   vertex rdd
  * @param edges
  *   edge rdd
  * @tparam VD
  *   vd
  * @tparam ED
  *   ed
  */
class GrapeGraphImpl[VD: ClassTag, ED: ClassTag] protected (
    @transient val vertices: GrapeVertexRDD[VD],
    @transient val edges: GrapeEdgeRDD[ED]
) extends Graph[VD, ED]
    with Serializable {
  lazy val backend: GraphStructureType = vertices.grapePartitionsRDD
    .mapPartitions(iter => {
      val part = iter.next()
      Iterator(part.graphStructure.structureType)
    })
    .collect()
    .distinct(0)
  lazy val fragmentIds: RDD[String] = {

    //we only use numFrags partitions, each with number of `numThread` for parallelization.
    PartitionAwareZippedBaseRDD.zipPartitions(
      SparkContext.getOrCreate(),
      edges.grapePartitionsRDD,
      grapeVertices.grapePartitionsRDD
    ) { (edgeIter, vertexIter) =>
      {
        val ePart = edgeIter.next()
        val vPart = vertexIter.next()
        require(ePart.pid == vPart.pid)
        if (ePart.localId != 0) {
          Iterator.empty
        } else {
          ePart.graphStructure match {
            case casted: FragmentStructure =>
              Iterator(
                ExecutorUtils.getHostName + ":" + ePart.pid + ":" + casted.mapToNewFragment(
                  vPart.vertexData,
                  ePart.edatas,
                  client = vPart.client
                )
              )
          }
        }
      }
    }
  }
  @transient override lazy val triplets: RDD[EdgeTriplet[VD, ED]] = {
    PartitionAwareZippedBaseRDD.zipPartitions(
      SparkContext.getOrCreate(),
      grapeEdges.grapePartitionsRDD,
      grapeVertices.grapePartitionsRDD
    ) { (edgeIter, vertexIter) =>
      {
        val edgePart   = edgeIter.next()
        val vertexPart = vertexIter.next()
        edgePart.tripletIterator(vertexPart.vertexData)
      }
    }
  }
  val logger: Logger =
    LoggerFactory.getLogger(classOf[GrapeGraphImpl[_, _]].toString)
  val vdClass: Class[VD] =
    classTag[VD].runtimeClass.asInstanceOf[java.lang.Class[VD]]
  val edClass: Class[ED] =
    classTag[ED].runtimeClass.asInstanceOf[java.lang.Class[ED]]
  val grapeEdges: GrapeEdgeRDDImpl[VD, ED] =
    edges.asInstanceOf[GrapeEdgeRDDImpl[VD, ED]]
  val grapeVertices: GrapeVertexRDDImpl[VD] =
    vertices.asInstanceOf[GrapeVertexRDDImpl[VD]]

  def numVertices: Long = grapeVertices.count()

  def numEdges: Long = edges.count()

  override def persist(newLevel: StorageLevel): Graph[VD, ED] = {
    vertices.persist(newLevel)
    edges.persist(newLevel)
    this
  }

  override def cache(): Graph[VD, ED] = {
    vertices.cache()
    edges.cache()
    this
  }

  override def checkpoint(): Unit = {
    vertices.checkpoint()
    edges.checkpoint()
  }

  override def isCheckpointed: Boolean = {
    vertices.isCheckpointed && edges.isCheckpointed
  }

  override def getCheckpointFiles: Seq[String] = {
    Seq(vertices.getCheckpointFile, edges.getCheckpointFile).flatMap {
      case Some(path) => Seq(path)
      case None       => Seq.empty
    }
  }

  override def unpersist(blocking: Boolean): Graph[VD, ED] = {
    vertices.unpersist(blocking)
    edges.unpersist(blocking)
    this
  }

  override def unpersistVertices(blocking: Boolean): Graph[VD, ED] = {
    vertices.unpersist(blocking)
    this
  }

  override def partitionBy(
      partitionStrategy: PartitionStrategy
  ): Graph[VD, ED] = {
    logger.warn("Currently grape graph doesn't support partition")
    this
  }

  override def partitionBy(
      partitionStrategy: PartitionStrategy,
      numPartitions: PartitionID
  ): Graph[VD, ED] = {
    logger.warn("Currently grape graph doesn't support partition")
    this
  }
  def mapVertices[VD2: ClassTag](
      map: (VertexId, VD) => VD2
  )(implicit eq: VD =:= VD2 = null): Graph[VD2, ED] = {
    new GrapeGraphImpl[VD2, ED](vertices.mapVertices[VD2](map), edges)
  }
  override def mapEdges[ED2: ClassTag](map: Edge[ED] => ED2): Graph[VD, ED2] = {
    val newEdgePartitions =
      grapeEdges.grapePartitionsRDD.mapPartitions(iter => {
        if (iter.hasNext) {
          val part = iter.next()
          Iterator(part.map(map))
        } else {
          Iterator.empty
        }
      })
    new GrapeGraphImpl[VD, ED2](
      vertices,
      grapeEdges.withPartitionsRDD(newEdgePartitions)
    )
  }

  override def mapEdges[ED2](
      f: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2]
  )(implicit evidence$5: ClassTag[ED2]): Graph[VD, ED2] = {
    val newEdges = grapeEdges.mapEdgePartitions(part => part.map(f))
    new GrapeGraphImpl[VD, ED2](vertices, newEdges)
  }

  override def mapTriplets[ED2: ClassTag](
      map: EdgeTriplet[VD, ED] => ED2
  ): Graph[VD, ED2] = {
    mapTriplets(map, TripletFields.All)
  }

  override def mapTriplets[ED2: ClassTag](
      map: EdgeTriplet[VD, ED] => ED2,
      tripletFields: TripletFields
  ): Graph[VD, ED2] = {
    //broadcast outer vertex data
    //After map vertices, broadcast new inner vertex data to outer vertex data
    var newVertices = vertices
    if (!grapeVertices.outerVertexSynced) {
      newVertices = grapeVertices.syncOuterVertex
    } else {
      logger.info(
        s"${grapeVertices} has done outer vertex data sync, just go to map triplets"
      )
    }

    val newEdgePartitions = PartitionAwareZippedBaseRDD.zipPartitions(
      SparkContext.getOrCreate(),
      grapeEdges.grapePartitionsRDD,
      newVertices.grapePartitionsRDD
    ) { (eIter, vIter) =>
      {
        if (vIter.hasNext) {
          val vPart = vIter.next()
          val epart = eIter.next()
          Iterator(
            epart.mapTriplets(
              map,
              vPart.bitSet,
              vPart.vertexData,
              tripletFields
            )
          )
        } else Iterator.empty
      }
    }
    val newEdges = grapeEdges.withPartitionsRDD(newEdgePartitions)
    new GrapeGraphImpl[VD, ED2](newVertices, newEdges)
  }

  override def mapTriplets[ED2](
      f: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2],
      tripletFields: TripletFields
  )(implicit evidence$8: ClassTag[ED2]): Graph[VD, ED2] = {
    var newVertices = vertices
    if (!grapeVertices.outerVertexSynced) {
      newVertices = grapeVertices.syncOuterVertex
    } else {
      logger.info(
        s"${grapeVertices} has done outer vertex data sync, just go to map triplets"
      )
    }
    val newEdgePartitions = PartitionAwareZippedBaseRDD.zipPartitions(
      SparkContext.getOrCreate(),
      grapeEdges.grapePartitionsRDD,
      newVertices.grapePartitionsRDD
    ) { (eIter, vIter) =>
      {
        if (vIter.hasNext) {
          val vPart = vIter.next()
          val epart = eIter.next()
          Iterator(epart.mapTriplets(f, vPart.vertexData, true, true))
        } else Iterator.empty
      }
    }
    val newEdges = grapeEdges.withPartitionsRDD(newEdgePartitions)
    new GrapeGraphImpl[VD, ED2](newVertices, newEdges)
  }

  override def reverse: Graph[VD, ED] = {
    new GrapeGraphImpl(vertices, grapeEdges.reverse)
  }

  override def subgraph(
      epred: EdgeTriplet[VD, ED] => Boolean,
      vpred: (VertexId, VD) => Boolean
  ): Graph[VD, ED] = {
    var newVertices = vertices
    if (!grapeVertices.outerVertexSynced) {
      newVertices = grapeVertices.syncOuterVertex
    } else {
      logger.info(
        s"${grapeVertices} has done outer vertex data sync, just go to map triplets"
      )
    }
    newVertices = newVertices.mapGrapeVertexPartitions(_.filter(vpred))

    val newEdgePartitions = PartitionAwareZippedBaseRDD.zipPartitions(
      SparkContext.getOrCreate(),
      grapeEdges.grapePartitionsRDD,
      newVertices.grapePartitionsRDD
    ) { (eIter, vIter) =>
      {
        if (vIter.hasNext) {
          val vPart = vIter.next()
          val ePart = eIter.next()
          Iterator(ePart.filter(epred, vpred, vPart.vertexData))
        } else Iterator.empty
      }
    }
    val newEdges = grapeEdges.withPartitionsRDD(newEdgePartitions)
    new GrapeGraphImpl(newVertices, newEdges)
  }

  override def mask[VD2: ClassTag, ED2: ClassTag](
      other: Graph[VD2, ED2]
  ): Graph[VD, ED] = {
    val newVertices = grapeVertices.innerJoin(
      other.asInstanceOf[GrapeGraphImpl[VD, ED]].grapeVertices
    ) { (oid, v1, v2) => v1 }
    val newEdges = grapeEdges.innerJoin(
      other.asInstanceOf[GrapeGraphImpl[VD, ED]].grapeEdges
    ) { (src, dst, e1, e2) => e1 }
    new GrapeGraphImpl(newVertices, newEdges)
  }

  override def groupEdges(merge: (ED, ED) => ED): Graph[VD, ED] = {
    new GrapeGraphImpl(
      vertices,
      grapeEdges.withPartitionsRDD(
        grapeEdges.grapePartitionsRDD.mapPartitions(iter => {
          if (iter.hasNext) {
            val part = iter.next()
            Iterator(part.groupEdges(merge))
          } else Iterator.empty
        })
      )
    )
  }

  override def outerJoinVertices[U: ClassTag, VD2: ClassTag](
      other: RDD[(VertexId, U)]
  )(
      mapFunc: (VertexId, VD, Option[U]) => VD2
  )(implicit eq: VD =:= VD2 = null): Graph[VD2, ED] = {
    val newVertices = vertices.leftJoin[U, VD2](other)(mapFunc)
    new GrapeGraphImpl[VD2, ED](newVertices, edges)
  }

  def generateDegreeRDD(edgeDirection: EdgeDirection): GrapeVertexRDD[Int] = {
    val newVertexPartitionRDD = PartitionAwareZippedBaseRDD.zipPartitions(
      SparkContext.getOrCreate(),
      grapeEdges.grapePartitionsRDD,
      grapeVertices.grapePartitionsRDD
    ) { (thisIter, otherIter) =>
      {
        if (thisIter.hasNext) {
          val ePart      = thisIter.next()
          val otherVPart = otherIter.next()
          //VertexPartition id range should be same with edge partition
          val newVdArray = ePart.getDegreeArray(edgeDirection)
          val vertexData = otherVPart.vertexData
          if (otherVPart.localId == 0) {
            val newValues =
              vertexData.mapToNew[Int].asInstanceOf[VertexDataStore[Int]]
            for (dstPid <- otherVPart.siblingPid) {
              VertexDataStore.enqueue(dstPid, newValues)
            }
          }
          val newValues = VertexDataStore
            .dequeue(otherVPart.pid)
            .asInstanceOf[VertexDataStore[Int]]
          require(
            (otherVPart.endLid - otherVPart.startLid) == newVdArray.length
          )
          //IN native graphx impl, the vertex with degree 0 is not returned. But we return them as well.
          //to make the result same, we set all vertices with zero degree to inactive.
          val startLid  = otherVPart.startLid
          val endLid    = otherVPart.endLid
          val activeSet = new BitSetWithOffset(startLid, endLid)
          activeSet.setRange(startLid, endLid)
          var i = startLid
          while (i < endLid) {
            newValues.set(i, newVdArray(i - startLid))
            i += 1
          }
          if (ePart.localNum == 0) {
            i = otherVPart.ivnum
            while (i < newValues.size) {
              newValues.set(i, 0) // for outer data, set 0.
              i += 1
            }
          }
          val newVPart = otherVPart.withNewValues(newValues)
          Iterator(newVPart.withMask(activeSet))
        } else Iterator.empty
      }
    }
    val degreeRDD =
      grapeVertices.withGrapePartitionsRDD(newVertexPartitionRDD).cache()
    logger.info(
      s"degree rdd size ${degreeRDD.count()}"
    ) // invoke calculation here to make sure the
    degreeRDD
  }

  override private[graphx] def aggregateMessagesWithActiveSet[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      activeSetOpt: Option[(VertexRDD[_], EdgeDirection)]
  ): VertexRDD[A] = {
    vertices.cache()
    val newVertices = activeSetOpt match {
      case Some((activeSet, _)) => {
        throw new IllegalStateException(
          "Currently not supported aggregate with active vertex set"
        )
      }
      case None => vertices.syncOuterVertex
    }

    val activeDirection = activeSetOpt.map(_._2)
    val preAgg = grapeEdges.grapePartitionsRDD
      .zipPartitions(newVertices.grapePartitionsRDD) { (eiter, viter) =>
        {
          val epart = eiter.next()
          val vpart = viter.next()
          epart.scanEdgeTriplet(
            vpart.vertexData,
            sendMsg,
            mergeMsg,
            tripletFields,
            activeDirection
          )
        }
      }
      .setName("GraphImpl.aggregateMessages - preAgg")

    newVertices.aggregateUsingIndex(preAgg, mergeMsg)
  }
}

object GrapeGraphImpl extends Logging {

  def fromExistingRDDs[VD: ClassTag, ED: ClassTag](
      vertices: GrapeVertexRDD[VD],
      edges: GrapeEdgeRDD[ED]
  ): GrapeGraphImpl[VD, ED] = {
    new GrapeGraphImpl[VD, ED](vertices.cache(), edges.cache())
  }
}
