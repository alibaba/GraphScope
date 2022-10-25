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

import org.apache.spark.internal.Logging

class IdParser(val fnum : Int)extends Logging{
  var fid_offset = 0
  var id_mask = 0
  init()

  def init() : Unit = {
    var maxFid = fnum - 1
    if (maxFid == 0){
      fid_offset = 63
    }
    else {
      var i = 0
      while (maxFid > 0){
        maxFid = (maxFid >>> 1)
        i += 1
      }
      fid_offset = 64 - i
    }
    id_mask = (1 << fid_offset) - 1
  }

  def getLocalId(gid : Long) : Long = {
    gid & id_mask
  }

  @inline
  def getFragId(gid : Long) : Int = {
    (gid >>> fid_offset).toInt
  }

  def generateGlobalId(fid : Int, lid : Long) : Long = {
    val fidLong = fid.toLong
    val res = (fidLong << fid_offset)
    res | lid
  }

}
