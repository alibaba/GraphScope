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

package org.apache.spark.sql

import com.alibaba.graphscope.format.{LongLong, LongLongInputFormat}
import com.alibaba.graphscope.graphx.GSClientWrapper
import com.alibaba.graphscope.graphx.GSClientWrapper.VINEYARD_DEFAULT_SHARED_MEM
import com.alibaba.graphscope.graphx.loader.FragmentLoader
import com.alibaba.graphscope.graphx.rdd.{FragmentRDD, LocationAwareRDD}
import com.alibaba.graphscope.graphx.shuffle._
import com.alibaba.graphscope.graphx.utils.{GSPartitioner, GrapeUtils, PrimitiveVector}
import org.apache.hadoop.io.LongWritable
import org.apache.spark.annotation.Stable
import org.apache.spark.graphx.grape.{GrapeEdgeRDD, GrapeGraphImpl, GrapeVertexRDD}
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx.scheduler.cluster.ExecutorInfoHelper
import org.apache.spark.graphx.{Graph, PartitionID, VertexId}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{ConfigEntry, EXECUTOR_ALLOW_SPARK_CONTEXT}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.internal._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.util.{CallSite, Utils}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, TaskContext}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.control.NonFatal

class GSSparkSession(sparkContext: SparkContext) extends SparkSession(sparkContext) {

  val sharedMemSize: String = {
    if (sparkContext.getConf.contains("spark.gs.vineyard.memory")) {
      sparkContext.getConf.get("spark.gs.vineyard.memory")
    } else VINEYARD_DEFAULT_SHARED_MEM
  }
  val userJarPath: String = {
    if (sparkContext.getConf.contains("spark.gs.submit.jar")) {
      sparkContext.getConf.get("spark.gs.submit.jar")
    } else
      throw new IllegalStateException(
        "please specify spark.gs.submit.jar to your app jar"
      )
  }

  /** On the creation of GSSpark session, we will start the python interpreter.
    */
  @transient val interpreter =
    new GSClientWrapper(sparkContext, socketPath, sharedMemSize)
  private val creationSite: CallSite = Utils.getCallSite()
  var socketPath: String = {
    if (sparkContext.getConf.contains("spark.gs.vineyard.sock")) {
      sparkContext.getConf.get("spark.gs.vineyard.sock")
    } else ""
  }
  if (socketPath.equals("")) {
    log.info(s"Update socket path from GraphScope: ${socketPath}")
    socketPath = interpreter.startedSocket
  }

  override def stop(): Unit = {
    super.stop()
    log.info("closing gs Spark session...")
    interpreter.close()
  }

  def loadGraphToGS[VD: ClassTag, ED: ClassTag](
      vFilePath: String,
      eFilePath: String,
      numPartitions: Int
  ): GrapeGraphImpl[VD, ED] = {
    interpreter.loadGraph(vFilePath, eFilePath, numPartitions)
  }

  /** Similar to methods defined in GraphLoader, same signature but read the edges to GraphScope store. Although
    * the Graph structure is stored in c++, with wrappers based on JNI, RDD can not differ GS-based RDD with graphx
    * rdd in java heap.
    */
  def edgeListFile(
      sc: SparkContext,
      path: String,
      canonicalOrientation: Boolean = false,
      numEdgePartitions: Int = -1,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  ): Graph[Int, Int] = {
    val lines = {
      if (numEdgePartitions > 0) {
        sc.hadoopFile(
          path,
          classOf[LongLongInputFormat],
          classOf[LongWritable],
          classOf[LongLong]
        ).setName(path) //coalesce(fakeNumPartitions)
      } else {
        sc.hadoopFile(
          path,
          classOf[LongLongInputFormat],
          classOf[LongWritable],
          classOf[LongLong]
        ).setName(path)
      }
    }.map(pair => (pair._2.first, pair._2.second)).cache()
    fromLineRDD(lines, numEdgePartitions, vertexStorageLevel, edgeStorageLevel)
  }

  def fromLineRDD(
      lines: RDD[(Long, Long)],
      numPartitions: Int,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  ): Graph[Int, Int] = {
    val sc                = SparkContext.getOrCreate()
    val partPartitioner   = new HashPartitioner(numPartitions)
    val rangePartitioner1 = new GSPartitioner[Long](numPartitions)
    val dataShuffles = lines
      .mapPartitionsWithIndex((fromPid, iter) => {
        //        iter.toArray
        val pid2src = Array.fill(numPartitions)(
          new org.apache.spark.util.collection.PrimitiveVector[VertexId](1200)
        )
        val pid2Dst = Array.fill(numPartitions)(
          new org.apache.spark.util.collection.PrimitiveVector[VertexId](1200)
        )
        val pid2Oids =
          Array.fill(numPartitions)(new OpenHashSet[VertexId](6000))
        //        val pid2OuterIds = Array.fill(numFrag)(new OpenHashSet[VertexId](6000))
        val time0 = System.nanoTime();
        while (iter.hasNext) {
          val line   = iter.next()
          val srcId  = line._1
          val dstId  = line._2
          val srcPid = partPartitioner.getPartition(srcId)
          val dstPid = partPartitioner.getPartition(dstId)
          pid2src(srcPid).+=(srcId)
          pid2Dst(srcPid).+=(dstId)
          if (srcPid != dstPid) {
            pid2src(dstPid) += (srcId)
            pid2Dst(dstPid) += (dstId)
          }
          pid2Oids(srcPid).add(srcId)
          pid2Oids(dstPid).add(dstId)
        }
        val time1 = System.nanoTime()
        log.info(
          "[edgeListFile: ] iterating over edge cost " + (time1 - time0) / 1000000 + "ms"
        )
        val res = new ArrayBuffer[(PartitionID, DataShuffle[Int, Int])]
        var ind = 0
        while (ind < numPartitions) {
          res += (
            (
              ind,
              new DefaultDataShuffle[Int, Int](
                ind,
                pid2Oids(ind),
                pid2src(ind).trim().array,
                pid2Dst(ind).trim().array,
                defaultVD = 1,
                defaultED = 1
              )
            )
          )
          ind += 1
        }
        val resIter = res.toIterator
        val time2   = System.nanoTime()
        log.info(
          "[edgeListFile: ] convert to iterator cost " + (time2 - time1) / 1000000 + "ms"
        )
        resIter
      })
      .partitionBy(rangePartitioner1)
      .cache()
    fromDataShuffle(numPartitions, sc, dataShuffles, edgeStorageLevel, vertexStorageLevel)
  }

  def fromDataShuffle[VD: ClassTag, ED: ClassTag](
      numPartitions: Int,
      sc: SparkContext,
      dataShuffles: RDD[(PartitionID, DataShuffle[VD, ED])],
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  ): GrapeGraphImpl[VD, ED] = {
    log.info("in fromDataShuffle")
    val executorInfo = ExecutorInfoHelper.getExecutorsHost2Id(sc)
    val executorNum  = executorInfo.size
    //construct
    //gather the host <-> partition id info.
    val (hostArray, pid2Host) = GrapeUtils.extractHostInfo(
      dataShuffles.asInstanceOf[RDD[(Int, _)]],
      numPartitions
    )
    val numWorker = hostArray.length
    val numFrag   = Math.min(numWorker, executorNum)
    log.info(
      s"numFrag ${numFrag}, num worker ${numWorker}, executor Num ${executorNum}"
    )

    //gather all received into one
    val gathered = dataShuffles
      .mapPartitions(
        iter => {
          if (iter.hasNext) {
            val edgeShuffleReceived = new DataShuffleHolder[VD, ED]()
            while (iter.hasNext) {
              val (pid, shuffle) = iter.next()
              if (shuffle != null) {
                edgeShuffleReceived.add(shuffle)
              }
            }
            Iterator(edgeShuffleReceived)
          } else {
            log.info("In gather encounter empty")
            Iterator.empty
          }
        },
        preservesPartitioning = true
      )
      .cache()
    log.info(s"gather all partition together size ${gathered.count()}")

    val graph = FragmentLoader.lineRDD2Graph(
      sc,
      numPartitions,
      numPartitions,
      gathered,
      numFrag,
      hostArray,
      executorInfo,
      vertexStorageLevel,
      edgeStorageLevel
    )
    log.info(
      s"constructed full graph size${graph.numVertices}, ${graph.numEdges}"
    )

    dataShuffles.unpersist()
    gathered.unpersist()

    graph
  }

  def loadFragmentAsGraph[VD: ClassTag, ED: ClassTag](
      sc: SparkContext,
      userNumPartitions: Int,
      objectIDs: String,
      fragName: String,
      vineyardSocket: String
  ): GrapeGraphImpl[VD, ED] = {
    val fragmentRDD = new FragmentRDD[VD, ED](
      sc,
      ExecutorInfoHelper.getExecutors(sc),
      fragName,
      objectIDs,
      vineyardSocket
    )
    val (vertexRDD, edgeRDD) = fragmentRDD.generateRDD(userNumPartitions)
    GrapeGraphImpl.fromExistingRDDs[VD, ED](vertexRDD, edgeRDD)
  }

}
object GSSparkSession extends Logging {

  private val listenerRegistered: AtomicBoolean = new AtomicBoolean(false)

  /** The active SparkSession for the current thread. */
  private val activeThreadSession = new InheritableThreadLocal[GSSparkSession]

  /** Reference to the root SparkSession. */
  private val defaultSession = new AtomicReference[GSSparkSession]
  private val HIVE_SESSION_STATE_BUILDER_CLASS_NAME =
    "org.apache.spark.sql.hive.HiveSessionStateBuilder"

  /** Creates a [[SparkSession.Builder]] for constructing a [[SparkSession]].
    *
    * @since 2.0.0
    */
  def builder(): Builder = new Builder

  /** Clears the active SparkSession for current thread. Subsequent calls to getOrCreate will return the first
    * created context instead of a thread-local override.
    *
    * @since 2.0.0
    */
  def clearActiveSession(): Unit = {
    activeThreadSession.remove()
  }

  /** Clears the default SparkSession that is returned by the builder.
    *
    * @since 2.0.0
    */
  def clearDefaultSession(): Unit = {
    defaultSession.set(null)
  }

  /** Returns the currently active SparkSession, otherwise the default one. If there is no default SparkSession,
    * throws an exception.
    *
    * @since 2.4.0
    */
  def active: SparkSession = {
    getActiveSession.getOrElse(
      getDefaultSession.getOrElse(
        throw new IllegalStateException(
          "No active or default Spark session found"
        )
      )
    )
  }

  /** Returns the active SparkSession for the current thread, returned by the builder.
    *
    * @note
    *   Return None, when calling this function on executors
    *
    * @since 2.2.0
    */
  def getActiveSession: Option[GSSparkSession] = {
    if (TaskContext.get != null) {
      // Return None when running on executors.
      None
    } else {
      Option(activeThreadSession.get)
    }
  }

  /** Changes the SparkSession that will be returned in this thread and its children when
    * SparkSession.getOrCreate() is called. This can be used to ensure that a given thread receives a SparkSession
    * with an isolated session, instead of the global (first created) context.
    *
    * @since 2.0.0
    */
  def setActiveSession(session: GSSparkSession): Unit = {
    activeThreadSession.set(session)
  }

  ////////////////////////////////////////////////////////////////////////////////////////
  // Private methods from now on
  ////////////////////////////////////////////////////////////////////////////////////////

  /** Returns the default SparkSession that is returned by the builder.
    *
    * @note
    *   Return None, when calling this function on executors
    *
    * @since 2.2.0
    */
  def getDefaultSession: Option[GSSparkSession] = {
    if (TaskContext.get != null) {
      // Return None when running on executors.
      None
    } else {
      Option(defaultSession.get)
    }
  }

  /** Sets the default SparkSession that is returned by the builder.
    *
    * @since 2.0.0
    */
  def setDefaultSession(session: GSSparkSession): Unit = {
    defaultSession.set(session)
  }

  def setExtensionsField(
      session: SparkSession,
      extensions: SparkSessionExtensions,
      initialSessionOptions: Map[String, String]
  ): SparkSession = {
    val extensionField = classOf[SparkSession].getDeclaredField("extensions")
    val initOptionField =
      classOf[SparkSession].getDeclaredField("initialSessionOptions")
    require(extensionField != null, "extension field null")
    require(initOptionField != null, "init option null")
    extensionField.setAccessible(true)
    initOptionField.setAccessible(true)

    extensionField.set(session, extensions)
    initOptionField.set(session, initialSessionOptions)
    session
  }

  /** Convert a spark graphx graph to gs fragment-based graph.
    * @param graph
    *   input graphx graph
    * @tparam VD
    *   vertex data type
    * @tparam ED
    *   edge data type
    * @return
    *   graphGraphImpl
    */
  def toGSGraph[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED]
  ): GrapeGraphImpl[VD, ED] = {
    graph match {
      case graphImpl: GrapeGraphImpl[VD, ED] => {
        log.info(s"${graph} is already a GS fragment-based graph")
        graphImpl
      }
      case graphImpl: GraphImpl[VD, ED] => {
        log.info(
          s"Converting graph of ${graphImpl.numVertices} vertices and ${graphImpl.numEdges} edges to GS fragment-based graph"
        )
        val time0 = System.nanoTime()
        val res   = graphXtoGSGraph(graphImpl)
        val time1 = System.nanoTime()
        log.info(
          s"[Perf:] Converting graph ${graphImpl} to grapeGraph ${res} cost ${(time1 - time0) / 1000000} ms"
        )
        res
      }
    }
  }

  def graphXtoGSGraph[VD: ClassTag, ED: ClassTag](
      originGraph: GraphImpl[VD, ED]
  ): GrapeGraphImpl[VD, ED] = {
    val numPartitions = originGraph.vertices.getNumPartitions
    val time0         = System.nanoTime()

    val graphShuffles = generateGraphShuffle(originGraph).cache()
    log.info(s"got graph shuffle of size ${graphShuffles.count()}")
    val session = GSSparkSession.getDefaultSession.getOrElse(throw new IllegalStateException("empty session"))
    val res     = session.fromDataShuffle(numPartitions, SparkContext.getOrCreate(), graphShuffles)
    graphShuffles.unpersist()
    res
  }

  def generateGraphShuffle[VD: ClassTag, ED: ClassTag](
      graph: GraphImpl[VD, ED]
  ): RDD[(PartitionID, DataShuffle[VD, ED])] = {
    val numPartitions    = graph.vertices.getNumPartitions
    val partPartitioner  = new HashPartitioner(numPartitions)
    val rangePartitioner = new GSPartitioner[Long](numPartitions)

    val shuffledVertexRDD = graph.vertices
      .mapPartitions(iter => {
        if (iter.hasNext) {
          val dstVertices = {
            Array.fill(numPartitions)(new org.apache.spark.util.collection.PrimitiveVector[Long])
          }
          val dstVertexData = Array.fill(numPartitions)(new org.apache.spark.util.collection.PrimitiveVector[VD])
          while (iter.hasNext) {
            val tuple = iter.next();
            val pid   = partPartitioner.getPartition(tuple._1)
            dstVertices(pid).+=(tuple._1)
            dstVertexData(pid).+=(tuple._2)
          }
          val res = new ArrayBuffer[(PartitionID, CustomDataShuffle[VD, ED])]
          var ind = 0
          while (ind < numPartitions) {
            res += (
              (
                ind,
                new CustomDataShuffle[VD, ED](ind, dstVertices(ind).trim().array, dstVertexData(ind).trim().array)
              )
            )
            ind += 1
          }
          res.toIterator
        } else Iterator.empty
      })
      .partitionBy(rangePartitioner)
      .cache()
    log.info(s"shuffled vertex rdd size ${shuffledVertexRDD.count()}")

    val shuffledEdgeRDD = graph.edges
      .mapPartitions(iter => {
        if (iter.hasNext) {
          val pid2src = Array.fill(numPartitions)(
            new org.apache.spark.util.collection.PrimitiveVector[VertexId]()
          )
          val pid2Dst = Array.fill(numPartitions)(
            new org.apache.spark.util.collection.PrimitiveVector[VertexId]()
          )
          val pid2Data = Array.fill(numPartitions)(
            new org.apache.spark.util.collection.PrimitiveVector[ED]()
          )
          while (iter.hasNext) {
            val edge   = iter.next();
            val srcId  = edge.srcId
            val dstId  = edge.dstId
            val data   = edge.attr
            val srcPid = partPartitioner.getPartition(srcId)
            pid2src(srcPid) += srcId
            pid2Dst(srcPid) += dstId
            pid2Data(srcPid) += data
          }
          val res = new ArrayBuffer[(PartitionID, CustomDataShuffle[VD, ED])]
          var ind = 0
          while (ind < numPartitions) {
            res += (
              (
                ind,
                new CustomDataShuffle[VD, ED](
                  ind,
                  pid2src(ind).trim().array,
                  pid2Dst(ind).trim().array,
                  pid2Data(ind).trim().array
                )
              )
            )
            ind += 1
          }
          res.toIterator
        } else Iterator.empty
      })
      .partitionBy(rangePartitioner)
      .cache()
    log.info(s"shuffled edge rdd count ${shuffledEdgeRDD.count()}")

    shuffledVertexRDD.union(shuffledEdgeRDD).cache().asInstanceOf[RDD[(PartitionID, DataShuffle[VD, ED])]]
  }

  /** Returns a cloned SparkSession with all specified configurations disabled, or the original SparkSession if all
    * configurations are already disabled.
    */
  private[sql] def getOrCloneSessionWithConfigsOff(
      session: SparkSession,
      configurations: Seq[ConfigEntry[Boolean]]
  ): SparkSession = {
    val configsEnabled =
      configurations.filter(session.sessionState.conf.getConf(_))
    if (configsEnabled.isEmpty) {
      session
    } else {
      val newSession = session.cloneSession()
      configsEnabled.foreach(conf => {
        newSession.sessionState.conf.setConf(conf, false)
      })
      newSession
    }
  }

  /** Register the AppEnd listener onto the Context */
  private def registerContextListener(sparkContext: SparkContext): Unit = {
    if (!listenerRegistered.get()) {
      sparkContext.addSparkListener(new SparkListener {
        override def onApplicationEnd(
            applicationEnd: SparkListenerApplicationEnd
        ): Unit = {
          defaultSession.set(null)
          listenerRegistered.set(false)
        }
      })
      listenerRegistered.set(true)
    }
  }

  private def sessionStateClassName(conf: SparkConf): String = {
    conf.get(CATALOG_IMPLEMENTATION) match {
      case "hive"      => HIVE_SESSION_STATE_BUILDER_CLASS_NAME
      case "in-memory" => classOf[SessionStateBuilder].getCanonicalName
    }
  }

  private def assertOnDriver(): Unit = {
    if (TaskContext.get != null) {
      // we're accessing it during task execution, fail.
      throw new IllegalStateException(
        "SparkSession should only be created and accessed on the driver."
      )
    }
  }

  /** Helper method to create an instance of `SessionState` based on `className` from conf. The result is either
    * `SessionState` or a Hive based `SessionState`.
    */
  private def instantiateSessionState(
      className: String,
      sparkSession: SparkSession,
      options: Map[String, String]
  ): SessionState = {
    try {
      // invoke new [Hive]SessionStateBuilder(
      //   SparkSession,
      //   Option[SessionState],
      //   Map[String, String])
      val clazz = Utils.classForName(className)
      val ctor  = clazz.getConstructors.head
      ctor
        .newInstance(sparkSession, None, options)
        .asInstanceOf[BaseSessionStateBuilder]
        .build()
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(
          s"Error while instantiating '$className':",
          e
        )
    }
  }

  /** @return
    *   true if Hive classes can be loaded, otherwise false.
    */
  private[spark] def hiveClassesArePresent: Boolean = {
    try {
      Utils.classForName(HIVE_SESSION_STATE_BUILDER_CLASS_NAME)
      Utils.classForName("org.apache.hadoop.hive.conf.HiveConf")
      true
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }

  private[spark] def cleanupAnyExistingSession(): Unit = {
    val session = getActiveSession.orElse(getDefaultSession)
    if (session.isDefined) {
      logWarning(
        s"""An existing Spark session exists as the active or default session.
           |This probably means another suite leaked it. Attempting to stop it before continuing.
           |This existing Spark session was created at:
           |
           |${session.get.creationSite.longForm}
           |
         """.stripMargin
      )
      session.get.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  /** Initialize extensions for given extension classnames. The classes will be applied to the extensions passed
    * into this function.
    */
  private def applyExtensions(
      extensionConfClassNames: Seq[String],
      extensions: SparkSessionExtensions
  ): SparkSessionExtensions = {
    extensionConfClassNames.foreach { extensionConfClassName =>
      try {
        val extensionConfClass = Utils.classForName(extensionConfClassName)
        val extensionConf = extensionConfClass
          .getConstructor()
          .newInstance()
          .asInstanceOf[SparkSessionExtensions => Unit]
        extensionConf(extensions)
      } catch {
        // Ignore the error if we cannot find the class or when the class has the wrong type.
        case e @ (_: ClassCastException | _: ClassNotFoundException | _: NoClassDefFoundError) =>
          logWarning(
            s"Cannot use $extensionConfClassName to configure session extensions.",
            e
          )
      }
    }
    extensions
  }

  /** Builder for [[GSSparkSession]].
    */
  @Stable
  class Builder extends Logging {

    private[this] val options =
      new scala.collection.mutable.HashMap[String, String]

    private[this] val extensions = new SparkSessionExtensions

    private[this] var userSuppliedContext: Option[SparkContext] = None

    /** Sets a name for the application, which will be shown in the Spark web UI. If no application name is set, a
      * randomly generated name will be used.
      *
      * @since 2.0.0
      */
    def appName(name: String): Builder = config("spark.app.name", name)

    /** Sets a config option. Options set using this method are automatically propagated to both `SparkConf` and
      * SparkSession's own configuration.
      *
      * @since 2.0.0
      */
    def config(key: String, value: String): Builder = synchronized {
      options += key -> value
      this
    }

    /** Sets a config option. Options set using this method are automatically propagated to both `SparkConf` and
      * SparkSession's own configuration.
      *
      * @since 2.0.0
      */
    def config(key: String, value: Long): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /** Sets a config option. Options set using this method are automatically propagated to both `SparkConf` and
      * SparkSession's own configuration.
      *
      * @since 2.0.0
      */
    def config(key: String, value: Double): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /** Sets a config option. Options set using this method are automatically propagated to both `SparkConf` and
      * SparkSession's own configuration.
      *
      * @since 2.0.0
      */
    def config(key: String, value: Boolean): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /** Sets a list of config options based on the given `SparkConf`.
      *
      * @since 2.0.0
      */
    def config(conf: SparkConf): Builder = synchronized {
      conf.getAll.foreach { case (k, v) => options += k -> v }
      this
    }

    /** Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]" to run locally with 4
      * cores, or "spark://master:7077" to run on a Spark standalone cluster.
      *
      * @since 2.0.0
      */
    def master(master: String): Builder = config("spark.master", master)

    /** GraphgScope related param, setting vineyard memroy size.
      */
    def vineyardMemory(memoryStr: String): Builder =
      config("spark.gs.vineyard.memory", memoryStr)

    /** GraphScope vineyard socket file. Vineyard process should be bound on this address on all workers.
      */
    def vineyardSock(filePath: String): Builder = {
      config("spark.gs.vineyard.sock", filePath)
    }

    /** User need to specify the file path to the jar submitted to spark cluster.
      */
    def gsSubmitJar(filePath: String): Builder = {
      config("spark.gs.submit.jar", filePath)
    }

    /** Enables Hive support, including connectivity to a persistent Hive metastore, support for Hive serdes, and
      * Hive user-defined functions.
      *
      * @since 2.0.0
      */
    def enableHiveSupport(): Builder = synchronized {
      if (hiveClassesArePresent) {
        config(CATALOG_IMPLEMENTATION.key, "hive")
      } else {
        throw new IllegalArgumentException(
          "Unable to instantiate SparkSession with Hive support because " +
            "Hive classes are not found."
        )
      }
    }

    /** Inject extensions into the [[SparkSession]]. This allows a user to add Analyzer rules, Optimizer rules,
      * Planning Strategies or a customized parser.
      *
      * @since 2.2.0
      */
    def withExtensions(f: SparkSessionExtensions => Unit): Builder =
      synchronized {
        f(extensions)
        this
      }

    /** Gets an existing [[SparkSession]] or, if there is no existing one, creates a new one based on the options
      * set in this builder.
      *
      * This method first checks whether there is a valid thread-local SparkSession, and if yes, return that one.
      * It then checks whether there is a valid global default SparkSession, and if yes, return that one. If no
      * valid global default SparkSession exists, the method creates a new SparkSession and assigns the newly
      * created SparkSession as the global default.
      *
      * In case an existing SparkSession is returned, the non-static config options specified in this builder will
      * be applied to the existing SparkSession.
      *
      * @since 2.0.0
      */
    def getOrCreate(): GSSparkSession = synchronized {
      val sparkConf = new SparkConf()
      options.foreach { case (k, v) => sparkConf.set(k, v) }

      if (!sparkConf.get(EXECUTOR_ALLOW_SPARK_CONTEXT)) {
        assertOnDriver()
      }

      // Get the session from current thread's active session.
      var session = activeThreadSession.get()
      if ((session ne null) && !session.sparkContext.isStopped) {
        applyModifiableSettings(session)
        return session
      }

      // Global synchronization so we will only set the default session once.
      GSSparkSession.synchronized {
        // If the current thread does not have an active session, get it from the global session.
        session = defaultSession.get()
        if ((session ne null) && !session.sparkContext.isStopped) {
          applyModifiableSettings(session)
          return session
        }

        // No active nor global default session. Create a new one.
        val sparkContext = userSuppliedContext.getOrElse {
          // set a random app name if not given.
          if (!sparkConf.contains("spark.app.name")) {
            sparkConf.setAppName(java.util.UUID.randomUUID().toString)
          }

          SparkContext.getOrCreate(sparkConf)
          // Do not update `SparkConf` for existing `SparkContext`, as it's shared by all sessions.
        }

        applyExtensions(
          sparkContext.getConf
            .get(StaticSQLConf.SPARK_SESSION_EXTENSIONS)
            .getOrElse(Seq.empty),
          extensions
        )

        session = new GSSparkSession(
          sparkContext
        ) // None, None, extensions, options.toMap
        GSSparkSession.setExtensionsField(session, extensions, options.toMap)
        setDefaultSession(session)
        setActiveSession(session)
        registerContextListener(sparkContext)
      }

      return session
    }

    private def applyModifiableSettings(session: SparkSession): Unit = {
      val (staticConfs, otherConfs) =
        options.partition(kv => SQLConf.staticConfKeys.contains(kv._1))

      otherConfs.foreach { case (k, v) =>
        session.sessionState.conf.setConfString(k, v)
      }

      if (staticConfs.nonEmpty) {
        logWarning(
          "Using an existing SparkSession; the static sql configurations will not take" +
            " effect."
        )
      }
      if (otherConfs.nonEmpty) {
        logWarning(
          "Using an existing SparkSession; some spark core configurations may not take" +
            " effect."
        )
      }
    }

    private[spark] def sparkContext(sparkContext: SparkContext): Builder =
      synchronized {
        userSuppliedContext = Option(sparkContext)
        this
      }
  }

}
