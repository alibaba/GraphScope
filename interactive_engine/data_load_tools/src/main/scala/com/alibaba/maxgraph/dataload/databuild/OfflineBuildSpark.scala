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
package com.alibaba.maxgraph.dataload.databuild

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

import java.io.FileInputStream
import java.io.IOException
import java.util.Properties
import scala.collection.mutable.ArrayBuffer

object OfflineBuildSpark {

  def process(confArray: Array[String],
              data: Array[Row],
              encodeData: ArrayBuffer[Tuple2[String, String]]) {
    for (i <- 0 until data.length) {
      val length = data(0).length
      val items:Array[String] = new Array[String](length)
      for (j <- 0 until length) {
        items(j) = data(i).get(j).toString()
      }
      val kv = DataBuildEncoder.encoder(confArray, items)
      if (kv.length == 2) {
        encodeData += new Tuple2(kv(0), kv(1));
      }
      else if (kv.length == 3) {
        encodeData += new Tuple2(kv(0), kv(2));
        encodeData += new Tuple2(kv(1), kv(2));
      }
      else {
        throw new Exception("encoded data with wrong size: " + kv.length)
      }
    }
  }

  def main(args: Array[String]) {

    val PARTITION_NUM = "partition.num"
    val PROJECT = "project"

    val OSS_ACCESS_ID = "oss.access.id"
    val OSS_ACCESS_KEY = "oss.access.key"
    val OSS_ENDPOINT = "oss.endpoint"
    val OSS_BUCKET_NAME = "oss.bucket.name"
    val OSS_OBJECT_NAME = "oss.object.name"

    val LDBC_CUSTOMIZE = "ldbc.customize"
    val SCHEMA_JSON = "schema.json"
    val COLUMN_MAPPINGS = "column.mappings"
    val VERTEX_TABLE_NAME = "vertexTableName"
    val EDGE_TABLE_NAME = "edgeTableName"

    val props = new Properties()

    val conf = new SparkConf().setAppName("DataBuild")

    try {
      props.load(new FileInputStream("config.init"))

      val ossAccessId = props.getProperty(OSS_ACCESS_ID)
      val ossAccessKey = props.getProperty(OSS_ACCESS_KEY)
      val ossEndpoint = props.getProperty(OSS_ENDPOINT)
      conf.set("spark.hadoop.fs.oss.accessKeyId", ossAccessId)
      conf.set("spark.hadoop.fs.oss.accessKeySecret", ossAccessKey)
      conf.set("spark.hadoop.fs.oss.endpoint", ossEndpoint)
      conf.set("spark.hadoop.fs.AbstractFileSystem.oss.impl", "com.aliyun.emr.fs.oss.OSS")
      conf.set("spark.hadoop.fs.oss.impl", "com.aliyun.emr.fs.oss.JindoOssFileSystem")
    } catch {
      case e: Throwable => throw e
    }

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext

    try {
      val confMap = OfflineBuildInit.init(props)
      val confArray:Array[String] = new Array[String](4)

      val partitionNum = confMap.get(PARTITION_NUM).toInt
      val project = confMap.get(PROJECT)
      confArray(0) = confMap.get(VERTEX_TABLE_NAME)
      confArray(1) = confMap.get(SCHEMA_JSON)
      confArray(2) = confMap.get(COLUMN_MAPPINGS)
      confArray(3) = confMap.get(LDBC_CUSTOMIZE)

      val encodeData = new ArrayBuffer[Tuple2[String, String]]()

      val vertexData = spark.sql("SELECT * FROM " + project + "." + confArray(0)).collect
      process(confArray, vertexData, encodeData)

      confArray(0) = confMap.get(EDGE_TABLE_NAME)

      val edgeData = spark.sql("SELECT * FROM " + project + "." + confArray(0)).collect
      process(confArray, edgeData, encodeData)

      val encodeDataArray = encodeData.toArray
      val encodeRDD = sc.parallelize(encodeDataArray, partitionNum).sortByKey()
      val ossBucketName = props.getProperty(OSS_BUCKET_NAME)
      val ossObjectName = props.getProperty(OSS_OBJECT_NAME)
      val ossOutputPath = "oss://" + ossBucketName + "/" + ossObjectName
      encodeRDD.saveAsTextFile(ossOutputPath)
    } catch {
      case e: Throwable => throw e
    }
    spark.close()
  }
}
