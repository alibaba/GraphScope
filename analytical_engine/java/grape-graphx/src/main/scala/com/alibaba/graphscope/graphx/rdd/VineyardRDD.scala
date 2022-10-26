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

package com.alibaba.graphscope.graphx.rdd

import com.alibaba.fastffi.{FFIByteString, FFITypeFactory}
import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.rdd.FragmentPartition.getHost
import com.alibaba.graphscope.graphx.utils.ScalaFFIFactory
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{GSSparkSession, SparkSession}
import org.apache.spark.{Partition, SparkContext, TaskContext}

import java.net.InetAddress

class VineyardPartition(val ind : Int,val hostName : String, val socketPath : String) extends Partition with Logging{
  lazy val client: VineyardClient = {
    if (hostName.equals(InetAddress.getLocalHost.getHostName)) {
      val res = ScalaFFIFactory.newVineyardClient()
      val ffiByteString: FFIByteString = FFITypeFactory.newByteString()
      ffiByteString.copyFrom(socketPath)
      res.connect(ffiByteString)
      log.info(s"successfully connect to ${socketPath}")
      res
    }
    else {
      log.info(s"This partition should be evaluated on this host since it is not on the desired host,desired host ${hostName}, cur host ${getHost}")
      null
    }
  }
  override def index: Int = ind
}

class VineyardRDD(sc : SparkContext, val locations : Array[String], val hostNames : Array[String]) extends RDD[VineyardClient](sc, Nil){
  val vineyardParts = new Array[VineyardPartition](locations.length)
  val sparkSession = GSSparkSession.getDefaultSession.getOrElse(throw new IllegalStateException("empty session")).asInstanceOf[GSSparkSession]
  for  (i <- vineyardParts.indices){
    vineyardParts(i) = new VineyardPartition(i,hostNames(i),sparkSession.socketPath)
  }
  override def compute(split: Partition, context: TaskContext): Iterator[VineyardClient] = {
    val casted = split.asInstanceOf[VineyardPartition]
    Iterator(casted.client)
  }

  override protected def getPartitions: Array[Partition] = {
    vineyardParts.asInstanceOf[Array[Partition]]
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val casted = split.asInstanceOf[VineyardPartition]
    log.info(s"get pref location for ${casted.hostName} ${casted.ind}")
    Array(locations(casted.ind))
  }
}
