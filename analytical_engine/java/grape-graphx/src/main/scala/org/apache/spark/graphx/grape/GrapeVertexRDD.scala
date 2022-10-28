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

import com.alibaba.graphscope.ds.Vertex
import com.alibaba.graphscope.fragment.IFragment
import com.alibaba.graphscope.graphx.graph.impl.FragmentStructure
import com.alibaba.graphscope.graphx.rdd.RoutingTable
import com.alibaba.graphscope.graphx.rdd.impl.GrapeVertexPartition
import com.alibaba.graphscope.graphx.store.impl.InHeapVertexDataStore
import com.alibaba.graphscope.utils.FFITypeFactoryhelper
import org.apache.spark.graphx._
import org.apache.spark.graphx.grape.impl.GrapeVertexRDDImpl
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Dependency, SparkContext}

import scala.reflect.ClassTag

/** Act as the base class of gs related rdds.
  */
abstract class GrapeVertexRDD[VD](sc: SparkContext, deps: Seq[Dependency[_]]) extends VertexRDD[VD](sc, deps) {
  override def partitionsRDD = null

  def mapVertices[VD2: ClassTag](
      map: (VertexId, VD) => VD2
  ): GrapeVertexRDD[VD2]

  override def innerZipJoin[U: ClassTag, VD2: ClassTag](other: VertexRDD[U])(
      f: (VertexId, VD, U) => VD2
  ): GrapeVertexRDD[VD2]

  override def innerJoin[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])(
      f: (VertexId, VD, U) => VD2
  ): GrapeVertexRDD[VD2]

  override def leftJoin[VD2: ClassTag, VD3: ClassTag](
      other: RDD[(VertexId, VD2)]
  )(f: (VertexId, VD, Option[VD2]) => VD3): GrapeVertexRDD[VD3]

  override def leftZipJoin[VD2: ClassTag, VD3: ClassTag](other: VertexRDD[VD2])(
      f: (VertexId, VD, Option[VD2]) => VD3
  ): VertexRDD[VD3]

  def syncOuterVertex: GrapeVertexRDD[VD]

  def collectNbrIds(direction: EdgeDirection): GrapeVertexRDD[Array[VertexId]]

  def updateAfterPIE[ED: ClassTag](): GrapeVertexRDD[VD]

  private[graphx] def grapePartitionsRDD: RDD[GrapeVertexPartition[VD]]

  private[graphx] def mapGrapeVertexPartitions[VD2: ClassTag](
      f: GrapeVertexPartition[VD] => GrapeVertexPartition[VD2]
  ): GrapeVertexRDD[VD2];

  private[graphx] def withGrapePartitionsRDD[VD2: ClassTag](
      partitionsRDD: RDD[GrapeVertexPartition[VD2]]
  ): GrapeVertexRDD[VD2]
}

object GrapeVertexRDD extends Logging {

  def fromVertexPartitions[VD: ClassTag](
      vertexPartition: RDD[GrapeVertexPartition[VD]]
  ): GrapeVertexRDDImpl[VD] = {
    new GrapeVertexRDDImpl[VD](vertexPartition)
  }

  def fromGrapeEdgeRDD[VD: ClassTag](
      edgeRDD: GrapeEdgeRDD[_],
      routingTable: RDD[RoutingTable],
      numPartitions: Int,
      defaultVal: VD,
      storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  ): GrapeVertexRDDImpl[VD] = {
    log.info(
      s"Driver: Creating vertex rdd from graphx edgeRDD of numPartition ${numPartitions}, default val ${defaultVal}"
    )
    val grapeVertexPartition = PartitionAwareZippedBaseRDD
      .zipPartitions(
        SparkContext.getOrCreate(),
        edgeRDD.grapePartitionsRDD,
        routingTable
      )((epartIter, routingIter) => {
        if (epartIter.hasNext) {
          val ePart = epartIter.next()
          require(routingIter.hasNext)
          val routingTable = routingIter.next()
          //vertex partition include the all the vertices in this frag, including inner vertices and outer vertices.
          log.debug(
            s"Vertex partition ${ePart.pid} doing initialization with default value ${defaultVal}, from ${ePart.startLid} to ${ePart.endLid}"
          )
          val grapeVertexPartition = GrapeVertexPartition.fromEdgePartition(
            defaultVal,
            ePart.pid,
            ePart.startLid,
            ePart.endLid,
            ePart.localId,
            ePart.localNum,
            ePart.siblingPid,
            ePart.client,
            ePart.graphStructure,
            routingTable
          )
          Iterator(grapeVertexPartition)
        } else Iterator.empty
      })
      .cache()
    new GrapeVertexRDDImpl[VD](grapeVertexPartition, storageLevel)
  }

  def fromFragmentEdgeRDD[VD: ClassTag](
      edgeRDD: GrapeEdgeRDD[_],
      routingTable: RDD[RoutingTable],
      numPartitions: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  ): GrapeVertexRDDImpl[VD] = {
    log.info(
      s"Driver: Creating vertex rdd from fragment edgeRDD of numPartition ${numPartitions}"
    )
    val grapeVertexPartitions = PartitionAwareZippedBaseRDD
      .zipPartitions(
        SparkContext.getOrCreate(),
        edgeRDD.grapePartitionsRDD,
        routingTable
      )((epartIter, routingIter) => {
        val ePart        = epartIter.next()
        val routingTable = routingIter.next()
        val vertexDataStore = new InHeapVertexDataStore[VD](
          ePart.graphStructure.getVertexSize.toInt,
          ePart.localNum,
          ePart.client
        )
        val actualStructure =
          ePart.graphStructure.asInstanceOf[FragmentStructure]
        val frag =
          actualStructure.fragment.asInstanceOf[IFragment[Long, Long, VD, _]]
        val vertex =
          FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
        for (i <- 0 until ePart.graphStructure.getInnerVertexSize.toInt) {
          vertex.SetValue(i)
          vertexDataStore.set(i, frag.getData(vertex))
        }
        //only set inner vertices
        val partition = new GrapeVertexPartition[VD](
          ePart.pid,
          0,
          frag.getInnerVerticesNum.toInt,
          ePart.localId,
          ePart.localNum,
          ePart.siblingPid,
          actualStructure,
          vertexDataStore,
          ePart.client,
          routingTable
        )
        Iterator(partition)
      })
      .cache()
    new GrapeVertexRDDImpl[VD](grapeVertexPartitions, storageLevel)
  }
}
