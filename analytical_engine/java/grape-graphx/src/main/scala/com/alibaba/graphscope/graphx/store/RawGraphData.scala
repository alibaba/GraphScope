package com.alibaba.graphscope.graphx.store

import com.alibaba.graphscope.graphx.shuffle.{
  CustomDataShuffle,
  DataShuffle,
  DataShuffleHolder,
  DefaultDataShuffle
}
import com.alibaba.graphscope.graphx.store.RawGraphData.{
  mergeInnerOids,
  mergeInnerOpenHashSet,
  processCustomShufflesToEdges,
  processDefaultShufflesToEdges
}
import com.alibaba.graphscope.graphx.utils.GrapeUtils.hashSet2Vector
import com.alibaba.graphscope.graphx.utils.{ExecutorUtils, GrapeUtils, ScalaFFIFactory}
import com.alibaba.graphscope.graphx.{GraphXRawData, VineyardClient}
import com.alibaba.graphscope.stdcxx.StdVector
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.OpenHashSet

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.ClassTag

/** Stores raw data,i.e. oid array and edge array, before fed to FragmentLoader.
  *
  * @tparam VD
  * @tparam ED
  */
class RawGraphData[VD: ClassTag, ED: ClassTag](
    val partitionID: Int,
    val partitionNum: Int,
    val vineyardClient: VineyardClient,
    val hostName: String,
    val parallelism: Int,
    val shuffleHolders: Array[DataShuffleHolder[VD, ED]]
) extends Logging {

  val time0: Long = System.nanoTime()
  val shuffles: Array[DataShuffle[VD, ED]] =
    shuffleHolders.flatMap(_.fromPid2Shuffle)

  /** Merge oids in several DataShuffle into one. Accelerate this procedure with multi-threading */
  val (oidArray, vdataArray): (StdVector[Long], StdVector[VD]) = {
    val size = shuffles.map(_.numVertices).sum
    log.info(
      s"Got ${size} vertices from ${shuffles.size} shuffles before merge"
    )

    val sampleShuffle = shuffles(0)
    val (mergedOidVector, mergedVdataVector): (StdVector[Long], StdVector[VD]) = sampleShuffle match {
      case defaultShuffle: DefaultDataShuffle[VD, ED] => {
        val mergedSet = mergeInnerOpenHashSet(
          shuffles.map(_.asInstanceOf[DefaultDataShuffle[VD, ED]].oids)
        )
        log.info(
          s"Got ${mergedSet.size} vertices from ${shuffles.size} shuffles after merge"
        )
        val oids = hashSet2Vector(mergedSet)

        val vdatas = GrapeUtils.buildVectorWithDefaultValue[VD](
          oids.size().toInt,
          defaultShuffle.getVdataIterator.next()
        )
        (oids, vdatas)
      }
      case customShuffle: CustomDataShuffle[VD, ED] => {
        val customShuffles = shuffles.map(_.asInstanceOf[CustomDataShuffle[VD, ED]])
        val (oidVector, vdataVector): (StdVector[Long], StdVector[VD]) = mergeInnerOids(
          customShuffles.filter(_.numVertices > 0).map(_.oids),
          customShuffles.filter(_.numVertices > 0).map(_.vdatas)
        )
        log.info(
          s"Got ${oidVector.size()}, ${vdataVector.size()} vertices from ${shuffles.length} shuffles after merge"
        )
        (oidVector, vdataVector)
      }
    }

    log.info("Successfully built oid and vdata vector")
    (mergedOidVector, mergedVdataVector)
  }
  log.info(
    s"Totally ${shuffles.length} shuffles from ${shuffleHolders.length} holders"
  )
  val (srcOidArray, dstOidArray, edataArray): (
      StdVector[Long],
      StdVector[Long],
      StdVector[ED]
  ) = {
    val size = shuffles.map(_.numEdges).sum
    log.info(
      s"Got ${size} edges from ${shuffles.size} shuffles before merge"
    )

    val sampleShuffle = shuffles(0)
    sampleShuffle match {
      case defaultShuffle: DefaultDataShuffle[VD, ED] => {
        val castedShuffle = new Array[DefaultDataShuffle[VD, ED]](shuffles.length)
        for (i <- shuffles.indices) {
          castedShuffle(i) = shuffles(i).asInstanceOf[DefaultDataShuffle[VD, ED]]
        }
        processDefaultShufflesToEdges(
          parallelism,
          castedShuffle
        )
      }
      case customDataShuffle: CustomDataShuffle[VD, ED] => {
        val customShuffles = shuffles.map(_.asInstanceOf[CustomDataShuffle[VD, ED]])
        processCustomShufflesToEdges(
          parallelism,
          customShuffles.filter(_.numEdges > 0)
        )
      }
    }
  }
  log.info("Successfully built edges")

  require(oidArray.size() == vdataArray.size(), s"vertex size neq ${oidArray.size()}, ${vdataArray.size()}")
  require(
    srcOidArray.size() == dstOidArray.size() && dstOidArray.size() == edataArray.size(),
    s"src dst edata neq ${srcOidArray.size()} ${dstOidArray.size()} ${edataArray.size()}"
  )

  val rawData: GraphXRawData[Long, Long, VD, ED] = {
    val builder = ScalaFFIFactory.newRawDataBuilder[Long, Long, VD, ED](
      vineyardClient,
      oidArray,
      vdataArray,
      srcOidArray,
      dstOidArray,
      edataArray
    )
    builder.seal(vineyardClient).get()
  }
  val time1: Long = System.nanoTime()
  log.info(
    s"Successfully built raw data ${rawData.id()}, cost ${(time1 - time0) / 1000000} ms"
  )
}

object RawGraphData extends Logging {

  def mergeInnerOpenHashSet(
      oidSets: Array[OpenHashSet[Long]]
  ): OpenHashSet[Long] = {
    val t0    = System.nanoTime()
    val queue = new ArrayBlockingQueue[OpenHashSet[Long]](8 * oidSets.length)
    for (i <- oidSets.indices) {
      require(queue.offer(oidSets(i)))
    }
    val coreNum = Math.min(queue.size(), 64)
    log.info(
      s"using ${coreNum} threads for local inner vertex join, queue size ${queue.size()}"
    )
    val threads = new Array[Thread](coreNum)
    for (i <- 0 until coreNum) {
      threads(i) = new Thread() {
        override def run(): Unit = {
          while (queue.size() > 1) {
            val tuple = this.synchronized {
              if (queue.size() > 1) {
                val first  = queue.take()
                val second = queue.take()
                if (first.size < second.size) {
                  (second, first)
                } else (first, second)
              } else null
            }
            if (tuple != null) {
              require(queue.offer(tuple._1.union(tuple._2)))
            }
          }
        }
      }
      threads(i).start()
    }
    for (i <- 0 until coreNum) {
      threads(i).join()
    }
    require(queue.size() == 1)
    val t1 = System.nanoTime()
    log.info(s"merge openHashSet cost ${(t1 - t0) / 1000000} ms")
    queue.take()
  }

  def mergeInnerOids[VD: ClassTag](
      oidsSet: Array[Array[Long]],
      vdatas: Array[Array[VD]]
  ): (StdVector[Long], StdVector[VD]) = {
    val t0      = System.nanoTime()
    val rawSize = oidsSet.map(_.size).sum
    if (vdatas.map(_.size).sum != rawSize) {
      throw new IllegalStateException(s"vdata size sum ${vdatas.map(_.size).sum} neq ${rawSize}")
    }
    val consumerNum = 64
    val consumers   = new Array[Thread](consumerNum)
    val index       = new AtomicInteger(0)
    val numArrays   = oidsSet.length

    val oidVector   = ScalaFFIFactory.newLongVector
    val vdataVector = ScalaFFIFactory.newVector[VD]
    log.info(
      s"parallel processing inner oids arr length ${numArrays}, raw size ${rawSize}"
    )
    oidVector.resize(rawSize)
    vdataVector.resize(rawSize)

    val offsets = new Array[Long](numArrays + 1)
    offsets(0) = 0
    for (i <- 0 until numArrays) {
      offsets(i + 1) = offsets(i) + oidsSet(i).length
    }

    for (i <- 0 until consumerNum) {
      consumers(i) = new Thread() {
        var flag = true
        while (flag) {
          val cur = index.getAndAdd(1)
          if (cur >= numArrays) {
            flag = false
          } else {
            val curOids   = oidsSet(cur)
            val curVdatas = vdatas(cur)
            require(
              curOids.length == curVdatas.length,
              s"inner array size neq ${curOids.length}, ${curVdatas.length}"
            )
            val begin      = offsets(cur)
            var innerIndex = 0
            while (innerIndex < curOids.length) {
              oidVector.set(begin + innerIndex, curOids(innerIndex))
              vdataVector.set(begin + innerIndex, curVdatas(innerIndex))
              innerIndex += 1
            }
          }
        }
      }
      consumers(i).start()
    }
    for (i <- 0 until consumerNum) {
      consumers(i).join()
    }

    val t1 = System.nanoTime()
    log.info(
      s"coalesce inner oid & vdatas  cost ${(t1 - t0) / 1000000} ms"
    )
    (oidVector, vdataVector)
  }

  def processDefaultShufflesToEdges[VD: ClassTag, ED: ClassTag](
      cores: Int,
      shuffles: Array[DefaultDataShuffle[VD, ED]]
  ): (StdVector[Long], StdVector[Long], StdVector[ED]) = {
    val rawEdgesNum = shuffles.map(_.numEdges).sum
    log.info(
      s"Got totally shuffle ${shuffles.length}, edges ${rawEdgesNum} in ${ExecutorUtils.getHostName}"
    )
    val srcOids = ScalaFFIFactory.newLongVector
    val dstOids = ScalaFFIFactory.newLongVector
    srcOids.resize(rawEdgesNum)
    dstOids.resize(rawEdgesNum)
    val time0 = System.nanoTime()

    val (srcArrays, dstArrays) =
      (shuffles.map(_.srcOids), shuffles.map(_.dstOids))
    var tid       = 0
    val atomicInt = new AtomicInteger(0)
    val outerSize = srcArrays.length
    log.info(s"use ${cores} threads for ${outerSize} shuffles")
    val threads = new Array[Thread](cores)
    val curInd  = new AtomicInteger(0)
    while (tid < cores) {
      val newThread = new Thread() {
        override def run(): Unit = {
          var flag = true
          while (flag) {
            var (got, myBegin) = synchronized {
              val got = atomicInt.getAndAdd(1);
              if (got >= outerSize) (-1, -1)
              else {
                val myBegin = curInd.getAndAdd(srcArrays(got).length)
                (got, myBegin)
              }
            }
            if (got < 0) {
              flag = false
            } else {
              val innerLimit = srcArrays(got).length
              require(dstArrays(got).length == innerLimit)
              val srcArray = srcArrays(got)
              val dstArray = dstArrays(got)
              val end      = myBegin + innerLimit
              var innerInd = 0
              while (myBegin < end) {
                srcOids.set(myBegin, srcArray(innerInd))
                dstOids.set(myBegin, dstArray(innerInd))
                myBegin += 1
                innerInd += 1
              }
            }
          }
        }
      }
      newThread.start()
      threads(tid) = newThread
      tid += 1
    }
    for (i <- 0 until cores) {
      threads(i).join()
    }
    require(curInd.get() == rawEdgesNum, s"neq ${curInd.get()} ${rawEdgesNum}")

    val time1 = System.nanoTime()
    log.info(s"Preprocess edge array cost ${(time1 - time0) / 1000000}ms")
    log.info("Now building edata array")

    val sampleShuffle = shuffles(0)
    val eDataArray = sampleShuffle match {
      case defaultShuffle: DefaultDataShuffle[VD, ED] => {
        GrapeUtils.buildVectorWithDefaultValue[ED](
          srcOids.size().toInt,
          defaultShuffle.getEdataIterator.next()
        )
      }
    }
    (srcOids, dstOids, eDataArray)
  }

  def processCustomShufflesToEdges[VD: ClassTag, ED: ClassTag](
      cores: Int,
      shuffles: Array[CustomDataShuffle[VD, ED]]
  ): (StdVector[Long], StdVector[Long], StdVector[ED]) = {
    val rawEdgesNum  = shuffles.map(_.numEdges).sum
    val tmpEdgesNum1 = shuffles.map(_.dstOids.length).sum
    val tmpEdgesNum2 = shuffles.map(_.edatas.length).sum
    require(rawEdgesNum == tmpEdgesNum1, s"src and dst oids length neq ${rawEdgesNum}, ${tmpEdgesNum1}")
    require(rawEdgesNum == tmpEdgesNum2, s"src and dst oids length neq ${rawEdgesNum}, ${tmpEdgesNum2}")

    log.info(
      s"Got totally shuffle ${shuffles.length}, edges ${rawEdgesNum} in ${ExecutorUtils.getHostName}"
    )
    val srcOids     = ScalaFFIFactory.newLongVector
    val dstOids     = ScalaFFIFactory.newLongVector
    val eDataVector = ScalaFFIFactory.newVector[ED]

    srcOids.resize(rawEdgesNum)
    dstOids.resize(rawEdgesNum)
    eDataVector.resize(rawEdgesNum)
    val time0 = System.nanoTime()

    val (srcArrays, dstArrays, edataArrays) =
      (shuffles.map(_.srcOids), shuffles.map(_.dstOids), shuffles.map(_.edatas))
    var tid       = 0
    val atomicInt = new AtomicInteger(0)
    val outerSize = srcArrays.length
    log.info(s"use ${cores} threads for ${outerSize} shuffles")
    val threads = new Array[Thread](cores)
    val curInd  = new AtomicInteger(0)
    while (tid < cores) {
      val newThread = new Thread() {
        override def run(): Unit = {
          var flag = true
          while (flag) {
            var (got, myBegin) = synchronized {
              val got = atomicInt.getAndAdd(1);
              if (got >= outerSize) (-1, -1)
              else {
                val myBegin = curInd.getAndAdd(srcArrays(got).length)
                (got, myBegin)
              }
            }
            if (got < 0) {
              flag = false
            } else {
              val innerLimit = srcArrays(got).length
              require(dstArrays(got).length == innerLimit)
              val srcArray   = srcArrays(got)
              val dstArray   = dstArrays(got)
              val edataArray = edataArrays(got)
              val end        = myBegin + innerLimit
              var innerInd   = 0
              while (myBegin < end) {
                srcOids.set(myBegin, srcArray(innerInd))
                dstOids.set(myBegin, dstArray(innerInd))
                eDataVector.set(myBegin, edataArray(innerInd))
                myBegin += 1
                innerInd += 1
              }
            }
          }
        }
      }
      newThread.start()
      threads(tid) = newThread
      tid += 1
    }
    for (i <- 0 until cores) {
      threads(i).join()
    }
    require(curInd.get() == rawEdgesNum, s"neq ${curInd.get()} ${rawEdgesNum}")

    val time1 = System.nanoTime()
    log.info(s"Preprocess edge array cost ${(time1 - time0) / 1000000}ms")
    log.info("Now building edata array")

    (srcOids, dstOids, eDataVector)
  }
}
