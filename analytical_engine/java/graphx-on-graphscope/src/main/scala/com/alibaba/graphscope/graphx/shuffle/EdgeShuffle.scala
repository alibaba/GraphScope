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

import com.alibaba.graphscope.graphx.utils.PrimitiveVector
import org.apache.spark.HashPartitioner
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.OpenHashSet

import java.util.concurrent.ArrayBlockingQueue
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


class EdgeShuffle[VD : ClassTag,ED : ClassTag](val fromPid : Int, val dstPid :Int, val oidArray : Array[Long], val oidSet : OpenHashSet[Long], val srcs : Array[Long], val dsts : Array[Long],
                                               val attrs: Array[ED] = null,
                                               val vertexAttrs : Array[VD] = null) extends Serializable with Logging {
  def size() : Long = srcs.length

  override def toString: String = "EdgeShuffleV2:{from "+ fromPid +",to "+ dstPid +  ",srcs: " + srcs.length + ",dsts: " + dsts.length

  def shuffleInEdges(originalDstPid : Int, pid2Host : Array[Int], partitioner : HashPartitioner, srcBuffer: Array[PrimitiveVector[Long]], dstBuffer : Array[PrimitiveVector[Long]]) : Unit = {
    //from my self, generate shuffles to dst node. result a iterator with two elelments.
    require(originalDstPid == dstPid, s"neq ${originalDstPid}, ${dstPid}")
    var i = 0
    val myHostId = pid2Host(originalDstPid)

    var cnt = 0;
    while (i < srcs.length){
      val dstShuffleId = partitioner.getPartition(dsts(i))
      if (dstShuffleId != originalDstPid && pid2Host(dstShuffleId) != myHostId){
        srcBuffer(dstShuffleId).+=(srcs(i))
        dstBuffer(dstShuffleId).+=(dsts(i))
        cnt += 1
      }
      i += 1
    }
  }
}

class EdgeShuffleReceived[ED: ClassTag] extends Logging{
  val fromPid2Shuffle = new ArrayBuffer[EdgeShuffle[_,ED]]

  def add(edgeShuffle: EdgeShuffle[_,ED]): Unit = {
    fromPid2Shuffle.+=(edgeShuffle)
  }

  def getArrays: (Array[Array[Long]], Array[Array[Long]], Array[Array[ED]]) = {
    val size = fromPid2Shuffle.size
    val srcArrays = new Array[Array[Long]](size)
    val dstArrays = new Array[Array[Long]](size)
    val attrArrays = new Array[Array[ED]](size)
    var i = 0
    while (i < size){
      srcArrays(i) = fromPid2Shuffle(i).srcs
      dstArrays(i) = fromPid2Shuffle(i).dsts
      attrArrays(i) = fromPid2Shuffle(i).attrs
      i += 1
    }
    (srcArrays,dstArrays,attrArrays)
  }

  /**
   * How many edges in received by us
   */
  def totalSize() : Long = {
    var i = 0;
    var res = 0L;
    while (i < fromPid2Shuffle.size){
      res += fromPid2Shuffle(i).size()
      i += 1
    }
    res
  }

  override def toString: String ={
    var res = "EdgeShuffleReceived @Partition: "
    for (shuffle <- fromPid2Shuffle){
      res += s"(from ${shuffle.fromPid}, receive size ${shuffle.srcs.length});"
    }
    res
  }
}

object EdgeShuffleReceived extends Logging {
    val queue = new ArrayBlockingQueue[EdgeShuffleReceived[_]](1024000)
    var array = null.asInstanceOf[Array[EdgeShuffleReceived[_]]]
    val distinctPids = null.asInstanceOf[Array[Int]]

  def push(in : EdgeShuffleReceived[_]): Unit = {
    require(queue.offer(in))
  }

  def getArray : Array[EdgeShuffleReceived[_]] = {
    if (array == null) {
      log.info(s"convert to array size ${queue.size()}")
      array = new Array[EdgeShuffleReceived[_]](queue.size())
      queue.toArray[EdgeShuffleReceived[_]](array)
    }
    array
  }
}
