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

package com.alibaba.graphscope.graphx.store

import org.apache.spark.internal.Logging
import org.apache.spark.util.KnownSizeEstimationPorter

import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}
import scala.reflect.ClassTag

trait VertexDataStore[T] extends KnownSizeEstimationPorter {
  //The size of vertex data store is equal to frag vertices num.
  def size(): Int
  def get(ind: Int): T
  def set(ind: Int, vd: T): Unit
  def mapToNew[T2: ClassTag]: VertexDataStore[T2]
  def getLocalNum: Int
  def setLocalNum(localNum: Int): Unit
}

object VertexDataStore extends Logging {
  val map = new ConcurrentHashMap[Int, LinkedBlockingQueue[VertexDataStore[_]]]

  def enqueue(pid: Int, inHeapVertexDataStore: VertexDataStore[_]): Unit = {
    createQueue(pid)
    val q = map.get(pid)
    q.offer(inHeapVertexDataStore)
  }

  def dequeue(pid: Int): VertexDataStore[_] = {
    createQueue(pid)
    require(map.containsKey(pid), s"no queue available for ${pid}")
    val res = map.get(pid).take()
    res
  }

  def createQueue(pid: Int): Unit = synchronized {
    if (!map.containsKey(pid)) {
      map.put(pid, new LinkedBlockingQueue)
    }
  }
}
