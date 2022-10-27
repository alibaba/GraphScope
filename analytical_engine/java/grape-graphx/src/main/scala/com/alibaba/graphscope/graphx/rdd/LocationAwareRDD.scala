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

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

class EmptyPartition(val ind: Int, val hostName: String, val loc: String)
    extends Partition
    with Logging {
  override def index: Int = ind
}

class LocationAwareRDD(
    sc: SparkContext,
    val locations: Array[String],
    val hostNames: Array[String],
    val partitionsIds: Array[Int]
) extends RDD[EmptyPartition](sc, Nil) {
  val parts = new Array[EmptyPartition](locations.length)
  for (i <- parts.indices) {
    val ind = partitionsIds(i)
    parts(ind) = new EmptyPartition(ind, hostNames(i), locations(i))
  }
  override def compute(
      split: Partition,
      context: TaskContext
  ): Iterator[EmptyPartition] = {
    val casted = split.asInstanceOf[EmptyPartition]
    Iterator(casted)
  }

  override protected def getPartitions: Array[Partition] = {
    parts.asInstanceOf[Array[Partition]]
  }

  override protected def getPreferredLocations(
      split: Partition
  ): Seq[String] = {
    val casted = split.asInstanceOf[EmptyPartition]
    Array(casted.loc)
  }
}
