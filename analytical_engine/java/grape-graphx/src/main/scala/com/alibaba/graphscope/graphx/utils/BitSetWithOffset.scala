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

class BitSetWithOffset(
    val startBit: Int,
    val endBit: Int,
    val bitset: FixedBitSet
) {
  val size = endBit - startBit
  require(endBit >= startBit)

  def this(startBit: Int, endBit: Int) = {
    this(startBit, endBit, new FixedBitSet(endBit - startBit))
  }

  def set(bit: Int): Unit = {
    check(bit)
    bitset.set(bit - startBit)
  }

  /** [a,b) */
  def setRange(a: Int, b: Int): Unit = {
    require(
      a >= startBit && a < endBit,
      s"${a} out of range ${startBit},${endBit}"
    )
    require(
      b > a && b > startBit && b <= endBit,
      s"${a} out of range ${startBit},${endBit}"
    )
    var i     = a - startBit
    val limit = b - startBit
    while (i < limit) {
      bitset.set(i)
      i += 1
    }
  }

  def get(bit: Int): Boolean = {
    check(bit)
    bitset.get(bit - startBit)
  }

  def unset(bit: Int): Unit = {
    check(bit)
    bitset.unset(bit - startBit)
  }

  @inline
  def check(bit: Int): Unit = {
    require(
      bit >= startBit && bit <= endBit,
      s"index of range ${bit}, range [${startBit},${endBit})"
    )
  }

  def union(other: BitSetWithOffset): Unit = {
    require(
      size == other.size && (startBit == other.startBit) && (endBit == other.endBit),
      s"can not union between ${this.toString} and ${other.toString}"
    )
    bitset.union(other.bitset)
  }

  def &(other: BitSetWithOffset): BitSetWithOffset = {
    require(
      size == other.size && (startBit == other.startBit) && (endBit == other.endBit),
      s"can not union between ${this.toString} and ${other.toString}"
    )
    new BitSetWithOffset(startBit, endBit, bitset & other.bitset)
  }

  override def toString: String =
    "BitSetWithOffset(start=" + startBit + ",end=" + endBit;

  def cardinality(): Int = bitset.cardinality()

  def capacity: Int = bitset.capacity

  @inline
  def nextSetBit(bit: Int): Int = {
    val res = bitset.nextSetBit(bit - startBit)
    if (res < 0) res
    else res + startBit
  }

  def andNot(other: BitSetWithOffset): BitSetWithOffset = {
    require(
      size == other.size && (startBit == other.startBit) && (endBit == other.endBit),
      s"can not union between ${this.toString} and ${other.toString}"
    )
    new BitSetWithOffset(startBit, endBit, bitset.andNot(other.bitset))
  }
}
