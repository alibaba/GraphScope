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

import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, TimeUnit}
import scala.reflect.ClassTag

trait EdgeDataStore[T] extends KnownSizeEstimationPorter{
  def size() : Int
  def getWithEID(eid : Int) : T
  def getWithOffset(offset: Int): T
  def setWithEID(ind : Int, ed : T) : Unit
  def setWithOffset(ind : Int, ed: T) : Unit
  def mapToNew[T2 : ClassTag] : EdgeDataStore[T2]
  def getLocalNum : Int
  def setLocalNum(localNum : Int) : Unit
}

object EdgeDataStore extends Logging {
  val map = new ConcurrentHashMap[Int,LinkedBlockingQueue[EdgeDataStore[_]]]

  def enqueue(pid : Int, edgeStore: EdgeDataStore[_]) : Unit = {
    createQueue(pid)
    val q = map.get(pid)
    q.offer(edgeStore)
  }

  def dequeue(pid : Int) : EdgeDataStore[_] = {
    createQueue(pid)
    require(map.containsKey(pid), s"no queue available for ${pid}")
    val res = map.get(pid).take()
    res
  }

  def createQueue(pid : Int) : Unit = synchronized{
    if (!map.containsKey(pid)){
      map.put(pid, new LinkedBlockingQueue)
    }
  }

}
