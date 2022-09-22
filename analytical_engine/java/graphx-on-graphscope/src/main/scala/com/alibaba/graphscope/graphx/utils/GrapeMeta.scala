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

import com.alibaba.graphscope.graphx.rdd.RoutingTable
import com.alibaba.graphscope.graphx.rdd.impl.GrapeEdgePartitionBuilder
import com.alibaba.graphscope.graphx.store.EdgeDataStore
import com.alibaba.graphscope.graphx.{GraphXCSR, GraphXVertexMap, LocalVertexMap, VineyardClient}
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

class GrapeMeta[VD: ClassTag, ED: ClassTag](val partitionID: Int, val partitionNum : Int, val vineyardClient : VineyardClient, val hostName : String) extends Logging {

  var localVertexMap : LocalVertexMap[Long,Long] = null.asInstanceOf[LocalVertexMap[Long,Long]]
  var edgePartitionBuilder : GrapeEdgePartitionBuilder[VD,ED] = null.asInstanceOf[GrapeEdgePartitionBuilder[VD,ED]]
  var globalVMId : Long = -1
  var globalVM : GraphXVertexMap[Long,Long] = null.asInstanceOf[GraphXVertexMap[Long,Long]]
  var graphxCSR :GraphXCSR[Long] = null.asInstanceOf[GraphXCSR[Long]]
  var routingTable : RoutingTable = null.asInstanceOf[RoutingTable]
  var edataStore : EdgeDataStore[ED] = null.asInstanceOf[EdgeDataStore[ED]]

  def setLocalVertexMap(in : LocalVertexMap[Long,Long]): Unit ={
    this.localVertexMap = in
  }

  def setEdgePartitionBuilder(edgePartitionBuilder: GrapeEdgePartitionBuilder[VD,ED]) = {
    this.edgePartitionBuilder = edgePartitionBuilder
  }

  def setEdataStore(ed : EdgeDataStore[ED]) : Unit = {
    this.edataStore = ed
  }

  def setGlobalVM(globalVMId : Long) : Unit = {
    log.info(s"setting global vm id ${globalVMId}")
    this.globalVMId = globalVMId
  }

  def setGlobalVM(globalVM : GraphXVertexMap[Long,Long]) : Unit = {
    this.globalVM = globalVM
  }

  def setCSR(csr : GraphXCSR[Long]) : Unit = {
    this.graphxCSR = csr
  }

  def setRoutingTable(routingTable: RoutingTable) : Unit = {
    this.routingTable = routingTable
  }
}
