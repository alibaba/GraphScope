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

package org.apache.spark.graphx.scheduler.cluster

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, ExecutorInfo}
import org.apache.spark.scheduler.local.LocalSchedulerBackend

import java.net.InetAddress
import scala.collection.mutable

object ExecutorInfoHelper extends Logging {

  def getExecutors(sc: SparkContext): mutable.HashMap[String, String] = {
    val castedBackend =
      sc.schedulerBackend.asInstanceOf[CoarseGrainedSchedulerBackend]
    val allFields = castedBackend.getClass.getSuperclass.getDeclaredFields
    log.info(s"${allFields.mkString(",")}")
    val field = castedBackend.getClass.getSuperclass.getDeclaredField(
      "org$apache$spark$scheduler$cluster$CoarseGrainedSchedulerBackend$$executorDataMap"
    )
    require(field != null)
    field.setAccessible(true)
    val executorDataMap = field
      .get(castedBackend)
      .asInstanceOf[mutable.HashMap[String, ExecutorInfo]]
    require(executorDataMap != null)

    val res = new mutable.HashMap[String, String]()
    for (tuple <- executorDataMap) {
      val executorId = tuple._1
      val host = tuple._2.executorHost
      log.info(s"executor id ${executorId}, host ${host}")
      //here we got ip, cast to hostname
      val hostName = InetAddress.getByName(host).getHostName
      res.+=((executorId, hostName))
    }
    res
  }

  def getExecutorsHost2Id(sc: SparkContext): mutable.HashMap[String, String] = {
    //executor backend can be several kinds,
    sc.schedulerBackend match {
      case coarseGrainedSchedulerBackend: CoarseGrainedSchedulerBackend => {
        val castedBackend =
          sc.schedulerBackend.asInstanceOf[CoarseGrainedSchedulerBackend]
        val allFields = castedBackend.getClass.getSuperclass.getDeclaredFields
        //    log.info(s"${allFields.mkString(",")}")
        val field = castedBackend.getClass.getSuperclass.getDeclaredField(
          "org$apache$spark$scheduler$cluster$CoarseGrainedSchedulerBackend$$executorDataMap"
        )
        require(field != null)
        field.setAccessible(true)
        val executorDataMap = field
          .get(castedBackend)
          .asInstanceOf[mutable.HashMap[String, ExecutorInfo]]
        require(executorDataMap != null)

        val res = new mutable.HashMap[String, String]()
        for (tuple <- executorDataMap) {
          val executorId = tuple._1
          val host = tuple._2.executorHost
          log.info(s"executor id ${executorId}, host ${host}")
          //here we got ip, cast to hostname
          val hostName = InetAddress.getByName(host).getHostName
          if (res.contains(hostName)) {
            throw new IllegalStateException(
              s"host ${hostName} already contains executor ${res.get(hostName)}"
            )
          }
          res.+=((hostName, executorId))
        }
        res
      }
      case local: LocalSchedulerBackend => {
        val res = new mutable.HashMap[String, String]()
        res.+=((InetAddress.getLocalHost.getHostName, "0"))
        res
      }
    }

  }
}
