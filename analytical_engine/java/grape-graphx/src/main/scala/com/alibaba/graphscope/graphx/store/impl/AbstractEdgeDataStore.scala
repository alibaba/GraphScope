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

import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.store.EdgeDataStore
import com.alibaba.graphscope.graphx.utils.{EIDAccessor, GrapeUtils, ScalaFFIFactory}

import scala.reflect.ClassTag

abstract class AbstractEdgeDataStore[T](val length : Int, var localNum : Int, val client : VineyardClient,val eidAccessor: EIDAccessor) extends EdgeDataStore[T]{
  def mapToNew[T2 : ClassTag] : EdgeDataStore[T2] = {
      if (!GrapeUtils.isPrimitive[T2]) {
        new InHeapEdgeDataStore[T2](length, localNum, client, new Array[T2](length), eidAccessor)
      }
      else {
        new OffHeapEdgeDataStore[T2](length, localNum, client,eidAccessor,ScalaFFIFactory.newEdgeDataBuilder[T2](client,length))
      }
  }

  override def size() : Int = length;

  override def getLocalNum: Int = localNum

  override def setLocalNum(localNum : Int) : Unit = {
    this.localNum = localNum
  }
}
