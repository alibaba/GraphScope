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

import scala.reflect.ClassTag

class ArrayWithOffset[@specialized(Long, Int, Double, Float) T: ClassTag](
    val offset: Int,
    val array: Array[T]
) {

  @inline
  def apply(i: Int): T = array(i - offset)

  @inline
  def update(i: Int, value: T): Unit = {
    array.update(i - offset, value)
  }

  override def clone(): ArrayWithOffset[T] = {
    new ArrayWithOffset[T](offset, length)
  }

  def this(offset: Int, length: Int) {
    this(offset, new Array[T](length))
  }

  @inline
  def length: Int = array.length
}
