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

package com.alibaba.graphscope.graphx.shuffle

import com.alibaba.graphscope.graphx.shuffle.VertexShuffle.INIT_SIZE
import com.alibaba.graphscope.graphx.utils.PrimitiveVector
import com.alibaba.graphscope.stdcxx.StdVector
import com.alibaba.graphscope.utils.FFITypeFactoryhelper
import org.apache.spark.internal.Logging

class VertexShuffle(val dstPid: Int, val fromPid: Int, val array: Array[Long])
    extends Serializable {
  def size(): Int = array.size
}

class VertexShuffleBuilder(val dstPid: Int, val fromPid: Int) extends Logging {
  @transient val oidArray = new PrimitiveVector[Long](INIT_SIZE)

  def addOid(oid: Long): Unit = oidArray.+=(oid)

  def finish(): VertexShuffle = {
    new VertexShuffle(dstPid, fromPid, oidArray.trim().array)
  }
}

object VertexShuffle extends Logging {
  val INIT_SIZE = 4;
  val vectorFactory: StdVector.Factory[Long] = FFITypeFactoryhelper
    .getStdVectorFactory("std::vector<int64_t>")
    .asInstanceOf[StdVector.Factory[Long]]
  def toVector(vertexShuffle: VertexShuffle): StdVector[Long] = {
    val vector = vectorFactory.create()
    vector.resize(vertexShuffle.size())
    var i = 0;
    log.info(
      s"Writing vertex shuffle to vector ${vector} size ${vector.size()}"
    )
    val array = vertexShuffle.array
    while (i < array.size) {
      vector.set(i, array(i))
      i += 1
    }
    vector
  }
}
