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

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}

import java.io.{IOException, ObjectOutputStream}
import scala.reflect.ClassTag

/** In spark, we need use zipPartition to combine two rdd together. In Spark default impl, partitions
  * of two rdd will be scheduled to same host. However, in our cases, we don't want data to move cross
  * machines. So we create this rdd to make all partitions located on first rdd's preference.
  */
class ZippedPartitionsPartition(
    idx: Int,
    @transient private val rdds: Seq[RDD[_]],
    @transient val preferredLocations: Seq[String]
) extends Partition
    with Logging {

  override val index: Int = idx
  var partitionValues = rdds.map(rdd => rdd.partitions(idx))
  def partitions: Seq[Partition] = partitionValues

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit =
    Utils.tryOrIOException {
      // Update the reference to parent split at the time of task serialization
      partitionValues = rdds.map(rdd => rdd.partitions(idx))
      oos.defaultWriteObject()
    }
}

abstract class PartitionAwareZippedBaseRDD[V: ClassTag](
    sc: SparkContext,
    var rdds: Seq[RDD[_]],
    preservesPartitioning: Boolean = false
) extends RDD[V](sc, rdds.map(x => new OneToOneDependency(x))) {

  override val partitioner =
    if (preservesPartitioning) firstParent[Any].partitioner else None

  override def getPartitions: Array[Partition] = {
    val numParts = rdds.head.partitions.length
    if (!rdds.forall(rdd => rdd.partitions.length == numParts)) {
      throw new IllegalArgumentException(
        s"Can't zip RDDs with unequal numbers of partitions: ${rdds.map(_.partitions.length)}"
      )
    }
    Array.tabulate[Partition](numParts) { i =>
      val firstPartition = rdds(0).partitions(i)
      new ZippedPartitionsPartition(
        i,
        rdds,
        rdds(0).preferredLocations(firstPartition)
      )
    }
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    s.asInstanceOf[ZippedPartitionsPartition].preferredLocations
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }
}

class PartitionAwareZippedPartitionRDD2[A: ClassTag, B: ClassTag, V: ClassTag](
    sc: SparkContext,
    var f: (Iterator[A], Iterator[B]) => Iterator[V],
    var rdd1: RDD[A],
    var rdd2: RDD[B],
    preservesPartitioning: Boolean = false
) extends PartitionAwareZippedBaseRDD[V](
      sc,
      List(rdd1, rdd2),
      preservesPartitioning
    ) {

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f(
      rdd1.iterator(partitions(0), context),
      rdd2.iterator(partitions(1), context)
    )
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    f = null
  }
}
object PartitionAwareZippedBaseRDD {
  def zipPartitions[T: ClassTag, B: ClassTag, V: ClassTag](
      sc: SparkContext,
      rdd1: RDD[T],
      rdd2: RDD[B],
      preservesPartitioning: Boolean = true
  )(f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] = {
    new PartitionAwareZippedPartitionRDD2(
      sc,
      sc.clean(f),
      rdd1,
      rdd2,
      preservesPartitioning
    )
  }
}
