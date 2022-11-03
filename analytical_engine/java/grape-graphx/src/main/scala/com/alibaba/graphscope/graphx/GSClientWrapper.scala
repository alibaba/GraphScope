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

import com.alibaba.fastffi.FFITypeFactory
import com.alibaba.graphscope.graphx.GSClientWrapper._
import com.alibaba.graphscope.graphx.utils.GrapeUtils
import com.alibaba.graphscope.utils.PythonInterpreter
import org.apache.spark.SparkContext
import org.apache.spark.graphx.grape.GrapeGraphImpl
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.cluster.ExecutorInfoHelper
import org.apache.spark.sql.GSSparkSession

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/** A wrapper for GraphScope python session. socketPath indicate vineyard already started.
  */
class GSClientWrapper(
    sc: SparkContext,
    socketPath: String = "",
    val sharedMemSize: String
) extends Logging {

  val graph2GraphName: mutable.HashMap[GrapeGraphImpl[_, _], String] =
    new mutable.HashMap[GrapeGraphImpl[_, _], String]()
  val executorInfo: mutable.HashMap[String, ArrayBuffer[String]] = ExecutorInfoHelper.getExecutorsHost2Id(sc)
  val executorNum: Int                                           = executorInfo.values.toIterator.map(_.size).sum
  val hostsNum                                                   = executorInfo.keys.size
  val gsHostsArr: Array[String]                                  = executorInfo.keys.toArray.distinct.sorted
  log.info(s"Available executors ${executorInfo.toIterator.toArray
    .mkString(",")}, hosts str ${gsHostsArr.map(v => "\'" + v + "\'").mkString(",")}")
  val pythonInterpreter = new PythonInterpreter
  pythonInterpreter.init()
  pythonInterpreter.runCommand("import graphscope")
  pythonInterpreter.runCommand("graphscope.set_option(show_log=True)")
  //NOTICE: Please notice that graphscope cluster will launch one and only one process in each worker(host),
  //but in spark, we can have more than one executor on same host. Our solution works around find, since we
  //don't talk to spark, we talk to the vineyard server. So as long as there are enough shared memory, our
  //program is ok.
  var initSessionCmd: String =
    "sess = graphscope.session(cluster_type=\"hosts\", num_workers=" + hostsNum +
      ", hosts=" + arr2PythonArrStr(gsHostsArr)
  //vineyard socket
  if (socketPath != null && socketPath.nonEmpty) {
    initSessionCmd += ",vineyard_socket= \"" + socketPath + "\""
  }
  initSessionCmd += ",vineyard_shared_mem=\"" + sharedMemSize + "\")"
  log.info(s"init session with: ${initSessionCmd}")
  pythonInterpreter.runCommand(initSessionCmd)

  pythonInterpreter.getMatched("GraphScope coordinator service connected")
  log.info("Successfully start gs session")
  val startedSocket: String     = getStartedSocket(pythonInterpreter)
  val v6dClient: VineyardClient = createVineyardClient(startedSocket)
  log.info(s"sess start vineyard on ${startedSocket}")
  sc.getConf.set("spark.gs.vineyard.sock", startedSocket)

  /** @param vfilePath
    *   vertex file path
    * @param efilePath
    *   edge file path, should be property edge file.
    * @return
    */
  def loadGraph[VD: ClassTag, ED: ClassTag](
      vfilePath: String,
      efilePath: String,
      userNumPartitions: Int
  ): GrapeGraphImpl[VD, ED] = {
    val graphName      = generateGraphName()
    val createGraphCmd = graphName + " = sess.g()"
    pythonInterpreter.runCommand(createGraphCmd)
    val loadVertexCmd =
      graphName + " = " + graphName + ".add_vertices(\"" + vfilePath + "\",\"person\")"
    log.info(s"load vertex cmd: ${loadVertexCmd}")
    pythonInterpreter.runCommand(loadVertexCmd)
    val loadEdgeCmd =
      graphName + " = " + graphName + ".add_edges(\"" + efilePath + "\",label=\"knows\",src_label=\"person\",dst_label=\"person\")"
    log.info(s"load edge cmd: ${loadEdgeCmd}")
    pythonInterpreter.runCommand(loadEdgeCmd)

    //project
    val projectGraph = graphName + "_proj"
    val projectCmd =
      projectGraph + " = " + graphName + ".project(vertices={\"person\":[\"weight\"]}, edges={\"knows\" : [\"dist\"]})"
    log.info(s"project cmd: ${projectCmd}")
    pythonInterpreter.runCommand(projectCmd)

    //get simple
    val simpleGraph = graphName + "_simple"
    val simpleCmd   = simpleGraph + " = " + projectGraph + "._project_to_simple()"
    log.info(s"project to simple cmd: ${projectCmd}")
    pythonInterpreter.runCommand(simpleCmd)

    //get id cmd
    val getIdCmd =
      "\"res_str:\"+" + simpleGraph + ".template_str + \";\"+str(" + simpleGraph + ".vineyard_id)"
    log.info(s"get simple vineyard id cmd: ${getIdCmd}")
    pythonInterpreter.runCommand(getIdCmd)

    var rawRes = pythonInterpreter.getMatched(RES_PATTERN)
    if (rawRes.startsWith("\'") || rawRes.endsWith("\"")) {
      rawRes = rawRes.substring(1)
    }
    if (rawRes.endsWith("\'") || rawRes.endsWith("\"")) {
      rawRes = rawRes.substring(0, rawRes.length - 1)
    }
    val resStr = rawRes
      .substring(rawRes.indexOf(RES_PATTERN) + RES_PATTERN.length + 1)
      .trim
    log.info(s"res str ${resStr}")
    val splited = resStr.split(";")
    require(splited.length == 2, "result str can't be splited into two parts")
    val fragName    = splited(0)
    val fragGroupId = splited(1)
    log.info(s"frag name : ${fragName}")
    log.info(s"frag group id ${fragGroupId}")
    //as this graph can later be used to run in graphscope session, we need to keep the matching between
    //java object and vineyard objectId,

    val fragGroupObjId = fragGroupId.toLong
    val hostIdsStr =
      GrapeUtils.getHostIdStrFromFragmentGroup(v6dClient, fragGroupObjId)
    log.info(s"parse from group id ${fragGroupObjId}, res ${hostIdsStr}")

    val sparkSession: GSSparkSession = GSSparkSession.getDefaultSession
      .getOrElse(throw new IllegalStateException("empty session"))
      .asInstanceOf[GSSparkSession]
    val res = sparkSession.loadFragmentAsGraph[VD, ED](
      sc,
      userNumPartitions,
      hostIdsStr,
      fragName,
      socketPath
    )
    log.info(
      s"got grapeGraph ${res} from host frag ids ${hostIdsStr}, graph name ${fragName}"
    )
    graph2GraphName(res) = graphName
    res
  }

  def close(): Unit = {
    pythonInterpreter.close()
    log.info("GS session closed")
  }
}

object GSClientWrapper {
  val RES_PATTERN = "res_str";
  //A safe word which we append to the execution of python code, its appearance in
  // output stream, indicating command has been successfully executoed.
  val SAFE_WORD                   = "Spark-GraphScope-OK"
  val graphNameCounter            = new AtomicInteger(0)
  val VINEYARD_DEFAULT_SHARED_MEM = "10Gi"

  def arr2PythonArrStr(arr: Array[String]): String = {
    if (arr.length == 0) {
      throw new IllegalStateException("array of size 0 is impossible")
    } else "[" + arr.map(v => "\'" + v + "\'").mkString(",") + "]"
  }

  /** we need a str name as handler in gs.
    * @return
    */
  def generateGraphName(): String = {
    val ind = graphNameCounter.getAndAdd(1)
    "graph" + ind
  }

  def createVineyardClient(socket: String): VineyardClient = {
    val client = FFITypeFactory
      .getFactory(classOf[VineyardClient])
      .asInstanceOf[VineyardClient.Factory]
      .create()
    val ffiByteString = FFITypeFactory.newByteString()
    ffiByteString.copyFrom(socket)
    client.connect(ffiByteString)
    client
  }

  def getStartedSocket(client: PythonInterpreter): String = {
    val cmd = "sess.engine_config[\"vineyard_socket\"]"
    client.runCommand(cmd)
    val res = client.getResult
    res.replace("\'", "")
  }
}
