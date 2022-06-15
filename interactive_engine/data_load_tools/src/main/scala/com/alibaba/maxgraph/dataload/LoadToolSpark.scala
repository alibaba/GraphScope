/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.dataload

import org.apache.spark.sql.SparkSession

import java.io.IOException

object LoadToolSpark {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("LoadTool_Spark")
      .getOrCreate()

    val sc = spark.sparkContext

    try {
      val command = args(0)
      val path = args(1)
      val uniquePath = args(2)
      if ("ingest".equalsIgnoreCase(command)) {
        LoadTool.ingest(path, true, uniquePath);
      } else if ("commit".equalsIgnoreCase(command)) {
        LoadTool.commit(path, true, uniquePath);
      } else {
        throw new Exception("supported COMMAND: ingest / commit");
      }
    } catch {
      case e: Throwable => throw e
    }
    spark.close()
  }
}
