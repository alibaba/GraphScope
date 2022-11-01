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

package com.alibaba.RDDReaderTransfer

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.jdk.CollectionConverters._
import Array._

object RDDReader {
  val node_executors: Map[String, Int] = Map()
  def getExecutorId(hostName: String): Int = {
    this.synchronized {
      if (node_executors.contains(hostName)) {
        node_executors(hostName) = node_executors(hostName) + 1
      } else {
        node_executors += (hostName -> 0)
      }
      return node_executors(hostName)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDReader")
    val sc = new SparkContext(conf)

    val node_file_name = sc.getConf.get("vertex_file")
    val edge_file_name = sc.getConf.get("edge_file")

    val nodes = sc.textFile(node_file_name).map(line => line.split(","))
      .map(parts =>(parts.head.toLong,parts.drop(1)))

    val node_type = "tuple:long:Array,string"
    println(node_type)

    nodes.foreachPartition(iter => {
      val cur_host = RDDReadServer.getLocalHostLANAddress()
      val executor_id = getExecutorId(cur_host)
      val server = new RDDReadServer(executor_id, TaskContext.get.partitionId, iter.asJava, node_type, nodes.getNumPartitions)
      server.start()
      server.blockUntilShutdown()
    })
    println("node transfer over")

    val edges = sc.textFile(edge_file_name).map(line => line.split(","))
      .map(parts => (parts(0).toLong, parts(1).toLong, parts.drop(2)))

    val edge_type = "tuple:long:long:Array,string"
    println(edge_type)

    edges.foreachPartition(iter => {
      val cur_host = RDDReadServer.getLocalHostLANAddress()
      val executor_id = getExecutorId(cur_host)
      val server = new RDDReadServer(executor_id, TaskContext.get.partitionId, iter.asJava, edge_type, edges.getNumPartitions)
      server.start()
      server.blockUntilShutdown()
    })
    println("edge transfer over")
    println("graph transfer all over")
  }
}
