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

package com.alibaba.graphscope.graphx.utils

import com.alibaba.graphscope.graphx.utils.LongVectorBuilder.initSize
import com.alibaba.graphscope.stdcxx.StdVector

class LongVectorBuilder() {
  val data : StdVector[Long] = ScalaFFIFactory.newLongVector
  data.resize(initSize)
  var curSize = 0

  def add(value : Long) : Unit = {
    check
    data.set(curSize, value)
    curSize += 1
  }

  def finish() : StdVector[Long] = {
    data.resize(curSize)
    data
  }

  def check : Unit = {
    if (curSize >= data.size()){
      val oldSize = data.size()
      data.resize(oldSize * 2)
    }
  }
}
object LongVectorBuilder{
  val initSize = 64
}
