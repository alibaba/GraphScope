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

package com.alibaba.graphscope.graphx

import com.alibaba.graphscope.graphx.utils.GrapeUtils
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GrapeUtilsTest extends FunSuite{
  test("test1") {
    val array = PrimitiveArray.create(classOf[Long], 5)
    val index = PrimitiveArray.create(classOf[Long], 5)
    array.set(0,5)
    array.set(1,4)
    array.set(2,3)
    array.set(3,2)
    array.set(4,1)

    index.set(0,4)
    index.set(1,3)
    index.set(2,2)
    index.set(3,1)
    index.set(4,0)

    val res = GrapeUtils.rearrangeArrayWithIndex(array,index)
    assert(res.get(0) == 1)
    assert(res.get(1) == 2)
    assert(res.get(4) == 5)
  }
}
