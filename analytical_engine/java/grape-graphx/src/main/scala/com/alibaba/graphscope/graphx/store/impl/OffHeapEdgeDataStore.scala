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

package com.alibaba.graphscope.graphx.store.impl

import com.alibaba.graphscope.arrow.array.PrimitiveArrowArrayBuilder
import com.alibaba.graphscope.ds.PrimitiveTypedArray
import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.store.EdgeDataStore
import com.alibaba.graphscope.graphx.utils.{EIDAccessor, GrapeUtils, ScalaFFIFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.util.SizeEstimator

import scala.reflect.ClassTag

class OffHeapEdgeDataStore[ED: ClassTag](
    length: Int,
    localNum: Int,
    client: VineyardClient,
    eidAccessor: EIDAccessor,
    val arrowArrayBuilder: PrimitiveArrowArrayBuilder[ED]
) extends AbstractEdgeDataStore[ED](length, localNum, client, eidAccessor)
    with EdgeDataStore[ED]
    with Logging {
  require(GrapeUtils.isPrimitive[ED])

  def this(
      length: Int,
      localNum: Int,
      client: VineyardClient,
      eidAccessor: EIDAccessor
  ) {
    this(
      length,
      localNum,
      client,
      eidAccessor,
      ScalaFFIFactory.newPrimitiveArrowArrayBuilder[ED]
    )
    arrowArrayBuilder.reserve(length)
  }

  override def size: Int = length

  override def getWithOffset(offset: Int): ED = {
    val eid = eidAccessor.getEid(offset).toInt
    arrowArrayBuilder.getValue(offset)
  }

  override def setWithOffset(offset: Int, ed: ED): Unit = {
    val eid = eidAccessor.getEid(offset).toInt
    arrowArrayBuilder.set(eid, ed)
  }

  override def getWithEID(eid: Int): ED = arrowArrayBuilder.getValue(eid)

  override def setWithEID(ind: Int, ed: ED): Unit = arrowArrayBuilder.set(ind, ed)

  override def estimatedSize: Long = {
    val res =
      SizeEstimator.estimate(client) + SizeEstimator.estimate(arrowArrayBuilder) + 8
//    log.info(s"${this.toString} computing estimated size ${GrapeUtils.bytesToString(res)}")
    res
  }
}
class ImmutableOffHeapEdgeStore[ED: ClassTag](
    length: Int,
    localNum: Int,
    client: VineyardClient,
    eidAccessor: EIDAccessor,
    typedArray: PrimitiveTypedArray[ED]
) extends AbstractEdgeDataStore[ED](length, localNum, client, eidAccessor)
    with EdgeDataStore[ED]
    with Logging {

  require(GrapeUtils.isPrimitive[ED])

  override def size: Int = length

  override def getWithOffset(offset: Int): ED = {
    val eid = eidAccessor.getEid(offset).toInt
    typedArray.get(eid)
  }

  override def setWithOffset(offset: Int, ed: ED): Unit = {
    throw new IllegalStateException("Not modifiable")
  }

  override def getWithEID(eid: Int): ED = typedArray.get(eid)

  override def setWithEID(ind: Int, ed: ED): Unit = {
    throw new IllegalStateException("Not modifiable")
  }

  override def estimatedSize: Long = {
    val res =
      SizeEstimator.estimate(client) + SizeEstimator.estimate(typedArray) + 8
    res
  }
}
