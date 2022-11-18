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

import com.alibaba.graphscope.context.GraphXParallelAdaptorContext
import com.alibaba.graphscope.graphx.rdd.impl.GrapeVertexPartition
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.grape.{GrapeVertexRDD, PartitionAwareZippedBaseRDD}
import org.apache.spark.graphx.impl.ShippableVertexPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

/** This class should be construct from off heap memories, which is provided by graphscope.
  * @param partitionsRDD
  * @param targetStorageLevel
  * @param vdTag
  * @tparam VD
  */
class GrapeVertexRDDImpl[VD] private[graphx] (
    @transient override val grapePartitionsRDD: RDD[GrapeVertexPartition[VD]],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
    val outerVertexSynced: Boolean = false
) // indicate whether we have done outer vertex shuffling.
(implicit override protected val vdTag: ClassTag[VD])
    extends GrapeVertexRDD[VD](
      grapePartitionsRDD.context,
      List(new OneToOneDependency(grapePartitionsRDD))
    ) {

  setName("GrapeVertexRDDImpl")
  override val partitioner: Option[Partitioner] = grapePartitionsRDD.partitioner

  override def reindex(): VertexRDD[VD] = {
    this
  }

  override def compute(
      part: Partition,
      context: TaskContext
  ): Iterator[(VertexId, VD)] = {
    val p = firstParent[GrapeVertexPartition[VD]].iterator(part, context)
    if (p.hasNext) {
      p.next().iterator.map(_.copy())
    } else {
      Iterator.empty
    }
  }

  def mapVertices[VD2: ClassTag](
      map: (VertexId, VD) => VD2
  ): GrapeVertexRDD[VD2] = {
    this.mapGrapeVertexPartitions(_.map(map))
  }

  override private[graphx] def mapGrapeVertexPartitions[VD2: ClassTag](
      f: GrapeVertexPartition[VD] => GrapeVertexPartition[VD2]
  ): GrapeVertexRDD[VD2] = {
    val newPartitionsRDD = grapePartitionsRDD.mapPartitions(
      iter => {
        if (iter.hasNext) {
          val part = iter.next();
          Iterator(f.apply(part))
        } else {
          Iterator.empty
        }
      },
      preservesPartitioning = true
    )
    this.withGrapePartitionsRDD(newPartitionsRDD)
  }

  override private[graphx] def withGrapePartitionsRDD[VD2: ClassTag](
      partitionsRDD: RDD[GrapeVertexPartition[VD2]]
  ): GrapeVertexRDD[VD2] = {
    new GrapeVertexRDDImpl[VD2](partitionsRDD, this.targetStorageLevel)
  }

  override def count(): Long = {
    grapePartitionsRDD.map(_.bitSet.cardinality()).reduce(_ + _)
  }

  override def mapValues[VD2](
      f: VD => VD2
  )(implicit evidence$2: ClassTag[VD2]): VertexRDD[VD2] = {
    this.mapGrapeVertexPartitions(_.map((vid, attr) => f(attr)))
  }

  override def mapValues[VD2](
      f: (VertexId, VD) => VD2
  )(implicit evidence$3: ClassTag[VD2]): VertexRDD[VD2] = {
    this.mapGrapeVertexPartitions(_.map(f))
  }

  override def minus(other: RDD[(VertexId, VD)]): VertexRDD[VD] = {
    minus(this.aggregateUsingIndex(other, (a: VD, b: VD) => a))
  }

  override def minus(other: VertexRDD[VD]): VertexRDD[VD] = {
    other match {
      case other: GrapeVertexRDDImpl[VD] if this.partitioner == other.partitioner => {
        this.withGrapePartitionsRDD[VD](
          PartitionAwareZippedBaseRDD.zipPartitions(
            SparkContext.getOrCreate(),
            grapePartitionsRDD,
            other.grapePartitionsRDD
          ) { (thisIter, otherIter) =>
            if (thisIter.hasNext) {
              val thisPartition  = thisIter.next()
              val otherPartition = otherIter.next()
              require(
                thisPartition.pid == otherPartition.pid,
                "partition id should match"
              )
              Iterator(thisPartition.minus(otherPartition))
            } else Iterator.empty
          }
        )
      }
      case _ =>
        throw new IllegalArgumentException(
          "can only minus a grape vertex rdd now"
        )
    }
  }

  override def diff(other: RDD[(VertexId, VD)]): VertexRDD[VD] = {
    diff(this.aggregateUsingIndex(other, (a: VD, b: VD) => a))
  }

  override def diff(other: VertexRDD[VD]): VertexRDD[VD] = {
    val otherPartition = other match {
      case other: GrapeVertexRDDImpl[VD] if this.partitioner == other.partitioner =>
        other.grapePartitionsRDD
      case _ =>
        throw new IllegalArgumentException(
          "can only minus a grape vertex rdd now"
        )
    }
    val newPartitionsRDD = PartitionAwareZippedBaseRDD.zipPartitions(
      SparkContext.getOrCreate(),
      grapePartitionsRDD,
      otherPartition
    ) { (thisIter, otherIter) =>
      val thisTuple  = thisIter.next()
      val otherTuple = otherIter.next()
      require(thisTuple.pid == otherTuple.pid, "partition id should match")
      Iterator(thisTuple.diff(otherTuple))
    }
    this.withGrapePartitionsRDD(newPartitionsRDD)
  }

  /** To aggregate with us, the input rdd can be a offheap vertexRDDImpl or a common one.
    *   - For offHeap rdd ,just do the aggregation, since its partitioner is same with use, i.e. hashPartition with
    *     fnum = num of workers.
    *   - For common one, we need to make sure they share the same num of partitions. and then we repartition to
    *     size of excutors.
    * @param messages
    * @param reduceFunc
    * @tparam VD2
    * @return
    */
  override def aggregateUsingIndex[VD2: ClassTag](
      messages: RDD[(VertexId, VD2)],
      reduceFunc: (VD2, VD2) => VD2
  ): VertexRDD[VD2] = {
    val executorStatus = sparkContext.getExecutorMemoryStatus
    log.info(s"Current available executors ${executorStatus}")
    val numExecutors = executorStatus.size
    val shuffled     = messages.partitionBy(new HashPartitioner(numExecutors))
    val newPartitionsRDD = PartitionAwareZippedBaseRDD.zipPartitions(
      SparkContext.getOrCreate(),
      grapePartitionsRDD,
      shuffled
    ) { (thisIter, msgIter) =>
      {
        if (thisIter.hasNext) {
          val tuple        = thisIter.next();
          val newPartition = tuple.aggregateUsingIndex(msgIter, reduceFunc)
          Iterator(newPartition)
        } else {
          Iterator.empty
        }
      }
    }
    this.withGrapePartitionsRDD[VD2](newPartitionsRDD)
  }

  override def leftJoin[VD2: ClassTag, VD3: ClassTag](
      other: RDD[(VertexId, VD2)]
  )(f: (VertexId, VD, Option[VD2]) => VD3): GrapeVertexRDD[VD3] = {
    other match {
      case other: GrapeVertexRDDImpl[VD2] =>
        leftZipJoin[VD2, VD3](other)(f)
      case _ =>
        throw new IllegalArgumentException(
          "currently not support to join with non-grape vertex rdd"
        )
    }
  }

  override def leftZipJoin[VD2: ClassTag, VD3: ClassTag](
      other: VertexRDD[VD2]
  )(f: (VertexId, VD, Option[VD2]) => VD3): GrapeVertexRDD[VD3] = {
    val newPartitionsRDD = other match {
      case other: GrapeVertexRDDImpl[VD2] => {
        PartitionAwareZippedBaseRDD.zipPartitions(
          SparkContext.getOrCreate(),
          grapePartitionsRDD,
          other.grapePartitionsRDD
        ) { (thisIter, otherIter) =>
          val thisTuple  = thisIter.next()
          val otherTuple = otherIter.next()
          Iterator(thisTuple.leftJoin[VD2, VD3](otherTuple)(f))
        }
      }
    }
    this.withGrapePartitionsRDD(newPartitionsRDD)
  }

  override def innerJoin[U: ClassTag, VD2: ClassTag](
      other: RDD[(VertexId, U)]
  )(f: (VertexId, VD, U) => VD2): GrapeVertexRDD[VD2] = {
    other match {
      case other: GrapeVertexRDDImpl[U] => innerZipJoin(other)(f)
      case _ =>
        throw new IllegalArgumentException(
          "Current only support join with grape vertex rdd"
        );
    }
  }

  override def innerZipJoin[U: ClassTag, VD2: ClassTag](
      other: VertexRDD[U]
  )(f: (VertexId, VD, U) => VD2): GrapeVertexRDD[VD2] = {
    val newPartitionsRDD = other match {
      case other: GrapeVertexRDDImpl[U] => {
        PartitionAwareZippedBaseRDD.zipPartitions(
          SparkContext.getOrCreate(),
          grapePartitionsRDD,
          other.grapePartitionsRDD
        ) { (thisIter, otherIter) =>
          val thisTuple  = thisIter.next()
          val otherTuple = otherIter.next()
          Iterator(thisTuple.innerJoin(otherTuple)(f))
        }
      }
    }
    this.withGrapePartitionsRDD(newPartitionsRDD)
  }

  override def syncOuterVertex: GrapeVertexRDD[VD] = {
    if (outerVertexSynced) {
      log.info("Outer vertex already synced")
      return this
    }
    val time0 = System.nanoTime()
    val updateMessage = this.grapePartitionsRDD
      .mapPartitions(iter => {
        if (iter.hasNext) {
          val part = iter.next()
          part.generateVertexDataMessage
        } else {
          Iterator.empty
        }
      })
      .partitionBy(
        new HashPartitioner(this.grapePartitionsRDD.getNumPartitions)
      )
      .cache()

    val updatedVertexPartition = PartitionAwareZippedBaseRDD
      .zipPartitions(
        SparkContext.getOrCreate(),
        grapePartitionsRDD,
        updateMessage
      ) { (vIter, msgIter) =>
        {
          if (vIter.hasNext) {
            val vpart = vIter.next()
            Iterator(vpart.updateOuterVertexData(msgIter))
          } else Iterator.empty
        }
      }
      .cache()

    val unused = updatedVertexPartition.count();
    val time1 = System.nanoTime()
    log.info(s"[Perf: ]Sync vertices cost ${(time1 - time0) / 1000000} ms, size ${unused}")
    updateMessage.unpersist()
    this.withGrapePartitionsRDD(updatedVertexPartition, true)
  }

  private[graphx] def withGrapePartitionsRDD[VD2: ClassTag](
      partitionsRDD: RDD[GrapeVertexPartition[VD2]],
      synced: Boolean
  ): GrapeVertexRDD[VD2] = {
    new GrapeVertexRDDImpl[VD2](partitionsRDD, this.targetStorageLevel, synced)
  }

  override def reverseRoutingTables(): VertexRDD[VD] = {
    throw new IllegalStateException(
      "Inherited but not implemented, should not be used"
    )
  }

  override def withEdges(edges: EdgeRDD[_]): VertexRDD[VD] = {
    throw new IllegalStateException(
      "Inherited but not implemented, should not be used"
    )
  }

  override def collectNbrIds(
      direction: EdgeDirection
  ): GrapeVertexRDD[Array[VertexId]] = {
    val part = grapePartitionsRDD.mapPartitions(iter => {
      val part = iter.next()
      Iterator(part.collectNbrIds(direction))
    })
    this.withGrapePartitionsRDD(part)
  }

  override def updateAfterPIE[ED: ClassTag](): GrapeVertexRDD[VD] = {
    val filePathPrefix = GraphXParallelAdaptorContext.pathPrefix
    val partitions = grapePartitionsRDD
      .mapPartitions(iter => {
        if (iter.hasNext) {
          val part = iter.next()
          Iterator(part.updateAfterPIE[ED](filePathPrefix))
        } else Iterator.empty
      })
      .cache()
    this.withGrapePartitionsRDD(partitions)
  }

  override protected def getPartitions: Array[Partition] =
    grapePartitionsRDD.partitions

  override private[graphx] def withPartitionsRDD[VD2](
      partitionsRDD: RDD[ShippableVertexPartition[VD2]]
  )(implicit evidence$13: ClassTag[VD2]) = {
    throw new IllegalStateException(
      "Inherited but not implemented, should not be used, use withGrapePartitionsRDD instead"
    )
  }

  override private[graphx] def withTargetStorageLevel(
      newTargetStorageLevel: StorageLevel
  ) = {
    new GrapeVertexRDDImpl[VD](grapePartitionsRDD, newTargetStorageLevel)
  }

  override private[graphx] def shipVertexAttributes(
      shipSrc: Boolean,
      shipDst: Boolean
  ) = {
    throw new IllegalStateException(
      "Inherited but not implemented, should not be used"
    )
  }

  override private[graphx] def shipVertexIds() = {
    throw new IllegalStateException(
      "Inherited but not implemented, should not be used"
    )
  }

  override private[graphx] def mapVertexPartitions[VD2](
      f: ShippableVertexPartition[VD] => ShippableVertexPartition[VD2]
  )(implicit evidence$1: ClassTag[VD2]) = {
    throw new IllegalStateException("Not implemented")
  }
}
