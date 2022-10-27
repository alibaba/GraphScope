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

import com.alibaba.graphscope.graphx.utils.BitSetWithOffset
import org.apache.spark.internal.Logging

class RoutingTable(val pid2Lids: Array[BitSetWithOffset]) extends Serializable {
  val numPartitions: Int = pid2Lids.length

  def get(ind: Int): BitSetWithOffset = {
    pid2Lids(ind)
  }
}

object RoutingTable extends Logging {

  def fromMessage(
      myPid: Int,
      partitionNum: Int,
      startLid: Int,
      endLid: Int,
      iter: Iterator[(Int, (Int, Array[Long]))]
  ): RoutingTable = {
    val res = Array.fill(partitionNum)(new BitSetWithOffset(startLid, endLid))
    while (iter.hasNext) {
      val piece = iter.next()
      val dstPid = piece._1
      require(
        myPid == dstPid,
        s"receive wrong msg, mypid ${myPid}, dst pid ${dstPid}"
      )
      val fromPid = piece._2._1
      val arr = piece._2._2
      var i = 0
      while (i < arr.length) {
        val lid = arr(i).toInt
        require(
          lid >= startLid && lid < endLid,
          s"received lid ${lid} not in range of pid ${myPid}, from ${fromPid}, my range ${startLid}, ${endLid}"
        )
        res(fromPid).set(lid)
        i += 1
      }
    }
    new RoutingTable(res)
  }
}
