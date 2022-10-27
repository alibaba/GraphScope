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

import java.net.InetAddress

/** Stores info for partitions, executor and hostname. With these info, we can know which
  *
  * - Which partitions are in this executor.
  * - How many partitions are in this executor.
  * - The host name of this executor.
  */
object ExecutorUtils extends Logging {
  val vineyardEndpoint = "/tmp/vineyard.sock"

  def getHostName: String = InetAddress.getLocalHost.getHostName
  def getHostIp: String = InetAddress.getLocalHost.getHostAddress

}
