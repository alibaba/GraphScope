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

import com.alibaba.graphscope.graphx.utils.BitSetWithOffset
import org.apache.spark.util.collection.BitSet
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BitSetTest extends FunSuite{
  test("test1"){
    val bitSet = new BitSet(8546613)
    bitSet.setUntil(8546612)
    var ind = 0;
    val time0 = System.nanoTime()
    while (ind > 0){
      ind = bitSet.nextSetBit(ind + 1)
    }
    val time1 = System.nanoTime()
    System.out.println(s"Bitset iter cost ${(time1 - time0) / 1000000} ms")
  }

  test("test2"){
    val bitSet = new BitSet(1000)
    bitSet.setUntil(1000)
    assert(bitSet.cardinality() == 1000)
    assert(bitSet.get(999))
  }

  test("test3"){
    val myBitSet = new BitSetWithOffset(500,502)
    assert(myBitSet.cardinality() == 0)
    assert(myBitSet.bitset.cardinality() == 0)
    myBitSet.set(500)
    assert(myBitSet.get(500))
    assert(!myBitSet.get(501))
    myBitSet.set(501)
    assert(myBitSet.get(501))
  }
}
