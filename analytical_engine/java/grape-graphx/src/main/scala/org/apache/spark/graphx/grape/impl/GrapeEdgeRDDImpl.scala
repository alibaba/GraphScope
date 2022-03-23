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

package org.apache.spark.graphx.grape.impl

import com.alibaba.graphscope.graphx.rdd.impl.GrapeEdgePartition
import org.apache.spark.graphx._
import org.apache.spark.graphx.grape.{GrapeEdgeRDD, PartitionAwareZippedBaseRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{OneToOneDependency, Partition, Partitioner, TaskContext}

import scala.reflect.ClassTag

class GrapeEdgeRDDImpl[VD: ClassTag, ED: ClassTag] private[graphx] (
    @transient override val grapePartitionsRDD: RDD[GrapeEdgePartition[VD, ED]],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
) extends GrapeEdgeRDD[ED](
      grapePartitionsRDD.context,
      List(new OneToOneDependency(grapePartitionsRDD))
    ) {

  override val partitioner: Option[Partitioner] = grapePartitionsRDD.partitioner

  override def compute(
      part: Partition,
      context: TaskContext
  ): Iterator[Edge[ED]] = {
    val p = firstParent[GrapeEdgePartition[VD, ED]].iterator(part, context)
    if (p.hasNext) {
      p.next().iterator.map(_.copy())
    } else {
      Iterator.empty
    }
  }

  override def setName(_name: String): this.type = {
    if (grapePartitionsRDD.name != null) {
      grapePartitionsRDD.setName(grapePartitionsRDD.name + ", " + _name)
    } else {
      grapePartitionsRDD.setName(_name)
    }
    this
  }

  override def collect(): Array[Edge[ED]] = this.map(_.copy()).collect()
  setName("GrapeEdgeRDDImpl")

  override def persist(newLevel: StorageLevel): this.type = {
    grapePartitionsRDD.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = false): this.type = {
    grapePartitionsRDD.unpersist(blocking)
    this
  }

  override def cache(): this.type = {
    grapePartitionsRDD.persist(targetStorageLevel)
    this
  }

  override def getStorageLevel: StorageLevel =
    grapePartitionsRDD.getStorageLevel

  override def checkpoint(): Unit = {
    grapePartitionsRDD.checkpoint()
  }

  override def isCheckpointed: Boolean = {
    firstParent[(PartitionID, GrapeEdgePartition[VD, ED])].isCheckpointed
  }

  override def getCheckpointFile: Option[String] = {
    grapePartitionsRDD.getCheckpointFile
  }

  override def count(): Long = {
    grapePartitionsRDD.map(_.activeEdgeNum).fold(0)(_ + _)
  }

  override def mapValues[ED2: ClassTag](
      f: Edge[ED] => ED2
  ): GrapeEdgeRDDImpl[VD, ED2] = {
    mapEdgePartitions((part) => part.map(f))
  }

  def mapEdgePartitions[VD2: ClassTag, ED2: ClassTag](
      f: GrapeEdgePartition[VD, ED] => GrapeEdgePartition[VD2, ED2]
  ): GrapeEdgeRDDImpl[VD2, ED2] = {
    this.withPartitionsRDD[VD2, ED2](
      grapePartitionsRDD.mapPartitions(
        { iter =>
          if (iter.hasNext) {
            val ep = iter.next()
            Iterator(f(ep))
          } else {
            Iterator.empty
          }
        },
        preservesPartitioning = true
      )
    )
  }

  def withPartitionsRDD[VD2: ClassTag, ED2: ClassTag](
      partitionsRDD: RDD[GrapeEdgePartition[VD2, ED2]]
  ): GrapeEdgeRDDImpl[VD2, ED2] = {
    new GrapeEdgeRDDImpl[VD2, ED2](partitionsRDD, this.targetStorageLevel)
  }

  override def reverse: GrapeEdgeRDD[ED] = {
    mapEdgePartitions(partition => partition.reverse)
  }

  override def innerJoin[ED2: ClassTag, ED3: ClassTag](
      other: EdgeRDD[ED2]
  )(f: (VertexId, VertexId, ED, ED2) => ED3): GrapeEdgeRDD[ED3] = {
    val newPartitions = PartitionAwareZippedBaseRDD.zipPartitions(
      context,
      grapePartitionsRDD,
      other.asInstanceOf[GrapeEdgeRDD[ED2]].grapePartitionsRDD
    )({ (thisIter, otherIter) =>
      {
        if (thisIter.hasNext) {
          val thisEpart = thisIter.next()
          val otherEpart = otherIter.next()
          Iterator(thisEpart.innerJoin(otherEpart)(f))
        } else Iterator.empty
      }
    })
    this.withPartitionsRDD[VD, ED3](newPartitions)
  }

  override def withTargetStorageLevel(
      newTargetStorageLevel: StorageLevel
  ): GrapeEdgeRDDImpl[VD, ED] = {
    new GrapeEdgeRDDImpl[VD, ED](grapePartitionsRDD, newTargetStorageLevel)
  }

  override protected def getPartitions: Array[Partition] =
    grapePartitionsRDD.partitions
}
