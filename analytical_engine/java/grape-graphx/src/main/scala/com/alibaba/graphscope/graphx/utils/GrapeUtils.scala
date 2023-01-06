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

import com.alibaba.fastffi.FFITypeFactory
import com.alibaba.graphscope.arrow.array.{PrimitiveArrowArrayBuilder, StringArrowArrayBuilder}
import com.alibaba.graphscope.ds.StringTypedArray
import com.alibaba.graphscope.graphx._
import com.alibaba.graphscope.graphx.store.impl.{
  ImmutableOffHeapEdgeStore,
  InHeapEdgeDataStore,
  InHeapVertexDataStore,
  OffHeapEdgeDataStore
}
import com.alibaba.graphscope.graphx.store.{EdgeDataStore, VertexDataStore}
import com.alibaba.graphscope.serialization.{FFIByteVectorOutputStream, FakeFFIByteVectorInputStream}
import com.alibaba.graphscope.stdcxx._
import com.alibaba.graphscope.utils.ThreadSafeBitSet
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.SparkEnv
import org.apache.spark.graphx.PartitionID
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.OpenHashSet

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}
import java.lang.reflect.Method
import java.math.{MathContext, RoundingMode}
import java.net.{InetAddress, UnknownHostException}
import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object GrapeUtils extends Logging {
  val BATCH_SIZE = 4096

  def class2Int(value: Class[_]): Int = {
    if (value.equals(classOf[java.lang.Long]) || value.equals(classOf[Long])) {
      4
    } else if (value.equals(classOf[java.lang.Integer]) || value.equals(classOf[Int])) {
      2
    } else if (value.equals(classOf[java.lang.Double]) || value.eq(classOf[Double])) {
      7
    } else throw new IllegalArgumentException(s"unexpected class ${value}")
  }
  def bytesForType[VD: ClassTag](value: Class[VD]): Int = {
    if (value.equals(classOf[Long]) || value.equals(classOf[Double])) 8
    else if (value.eq(classOf[Int]) || value.equals(classOf[Float])) 4
    else throw new IllegalStateException("Unrecognized : " + value.getName)
  }

  def classToStr[T: ClassTag](signed: Boolean = true): String = {
    classToStr(getRuntimeClass[T], signed)
  }

  def getMethodFromClass[T](
      clz: Class[T],
      name: String,
      paramClasses: Class[_]
  ): Method = {
    val method = clz.getDeclaredMethod(name, paramClasses)
    method.setAccessible(true)
    require(method != null, "can not find method: " + name)
    method
  }

  def classToStr(value: Class[_], signed: Boolean): String = {
    if (value.equals(classOf[java.lang.Long]) || value.equals(classOf[Long])) {
      if (signed) "int64_t"
      else "uint64_t"
    } else if (value.equals(classOf[java.lang.Integer]) || value.equals(classOf[Int])) {
      if (signed) "int32_t"
      else "uint32_t"
    } else if (value.equals(classOf[java.lang.Double]) || value.eq(classOf[Double])) {
      "double"
    } else if (value.equals(classOf[Json])) {
      "vineyard::json"
    } else {
      "std::string"
    }
  }

  def scalaClass2JavaClass[T: ClassTag](vdClass: Class[T]): Class[_] = {
    if (vdClass.equals(classOf[Int])) {
      classOf[Integer];
    } else if (vdClass.equals(classOf[Long])) {
      classOf[java.lang.Long]
    } else if (vdClass.equals(classOf[Double])) {
      classOf[java.lang.Double]
    } else {
      throw new IllegalStateException(
        "transform failed for " + vdClass.getName
      );
    }
  }

  def isPrimitive[T: ClassTag]: Boolean = {
    val clz = getRuntimeClass[T]
    clz.equals(classOf[Double]) || clz.equals(classOf[Long]) || clz.equals(
      classOf[Int]
    ) || clz.equals(classOf[Float])
  }

  def isPrimitive(clz: Class[_]): Boolean = {
    clz.equals(classOf[Int]) || clz.equals(classOf[Long]) || clz.equals(classOf[Double]) || clz.equals(
      classOf[Float]
    ) ||
    clz.equals(classOf[java.lang.Integer]) || clz.equals(classOf[java.lang.Long]) || clz.equals(
      classOf[java.lang.Double]
    ) || clz.equals(classOf[java.lang.Float])
  }

  @throws[UnknownHostException]
  def getSelfHostName = InetAddress.getLocalHost.getHostName

  def dedup(files: Array[String]): Array[String] = {
    val set = files.toSet
    set.toArray
  }

  /** used to transform offset base edata store to eids based store.
    * @tparam T
    *   data type
    */
  def rearrangeArrayWithIndex[T: ClassTag](
      array: PrimitiveArray[T],
      index: PrimitiveArray[Long]
  ): PrimitiveArray[T] = {
    val len = array.size()
    require(
      index.size() == len,
      s"array size ${len} neq eids array ${index.size()}"
    )
    val newArray = PrimitiveArray
      .create(getRuntimeClass[T], len)
      .asInstanceOf[PrimitiveArray[T]]
    var i = 0
    while (i < len) {
      newArray.set(i, array.get(index.get(i)))
      i += 1
    }
    newArray
  }

  /** We can not copy all vertex data elements into arrow array builder, since array fragment only stores the inner
    * vertex data.
    */
  def vertexDataStore2ArrowArrayBuilder[T: ClassTag](
      vertexDataStore: VertexDataStore[T],
      ivnum: Int
  ): PrimitiveArrowArrayBuilder[T] = {
    require(GrapeUtils.isPrimitive[T])
    val array = vertexDataStore.asInstanceOf[InHeapVertexDataStore[T]].array
    fillPrimitiveArrowArrayBuilder(array, ivnum)
  }

  /** We can not copy all vertex data elements into arrow array builder, since array fragment only stores the inner
    * vertex data.
    */
  def vertexDataStore2ArrowStringArrayBuilder[T: ClassTag](
      vertexDataStore: VertexDataStore[T],
      ivnum: Int
  ): StringArrowArrayBuilder = {
    require(!GrapeUtils.isPrimitive[T])
    val array = vertexDataStore.asInstanceOf[InHeapVertexDataStore[T]].array
    fillComplexArrowArrayBuilder(array, ivnum)
  }

  def edgeDataStore2ArrowArrayBuilder[T: ClassTag](
      edgeDataStore: EdgeDataStore[T]
  ): PrimitiveArrowArrayBuilder[T] = {
    require(GrapeUtils.isPrimitive[T], "need to be primitive")
    edgeDataStore match {
      case inHeapEdgeDataStore: InHeapEdgeDataStore[T] => {
        val array = edgeDataStore.asInstanceOf[InHeapEdgeDataStore[T]].edataArray
        fillPrimitiveArrowArrayBuilder(array, array.length)
      }
      case offHeapEdgeDataStore: OffHeapEdgeDataStore[T] => {
        offHeapEdgeDataStore.arrowArrayBuilder
      }
      case immutableOffHeapEdgeStore: ImmutableOffHeapEdgeStore[T] => {
        log.info("Our rdd still use the original edata store in fragment, so no edata builder will created")
        null
      }
      case _ => throw new IllegalStateException("Unrecognized edge data store")
    }
  }
  def edgeDataStore2ArrowStringArrayBuilder[T: ClassTag](
      edgeDataStore: EdgeDataStore[T]
  ): StringArrowArrayBuilder = {
    require(!GrapeUtils.isPrimitive[T], "need to be complex")
    edgeDataStore match {
      case inHeapEdgeDataStore: InHeapEdgeDataStore[T] => {
        val array = edgeDataStore.asInstanceOf[InHeapEdgeDataStore[T]].edataArray
        fillComplexArrowArrayBuilder(array, array.length)
      }
      case offHeapEdgeDataStore: OffHeapEdgeDataStore[T] => {
        throw new IllegalStateException("Not possible")
      }
      case immutableOffHeapEdgeStore: ImmutableOffHeapEdgeStore[T] => {
        log.info("Our rdd still use the original edata store in fragment, so no edata builder will created")
        null
      }
      case _ => throw new IllegalStateException("Unrecognized edge data store")
    }
  }

  /** end indicates how many first elements we wants in the result builder. */
  def fillPrimitiveArrowArrayBuilder[T: ClassTag](
      array: Array[T],
      length: Int
  ): PrimitiveArrowArrayBuilder[T] = {
    require(length <= array.length, s"specified length ${length} ge array length ${array.length}")
    val size              = length
    var i                 = 0
    val arrowArrayBuilder = ScalaFFIFactory.newPrimitiveArrowArrayBuilder[T]
    arrowArrayBuilder.reserve(size)
    while (i < size) {
      arrowArrayBuilder.unsafeAppend(array(i))
      i += 1
    }
    arrowArrayBuilder
  }

  def fillComplexArrowArrayBuilder[T: ClassTag](
      array: Array[T],
      length: Int
  ): StringArrowArrayBuilder = {
    require(length <= array.length, s"specified length ${length} ge array length ${array.length}")
    val (buffer, offset) = serializeComplexArray(array)
    buffer.touch()
    val pointer =
      FFITypeFactory.getFactory(classOf[CCharPointer], "char").asInstanceOf[CCharPointer.Factory].create()
    pointer.setAddress(buffer.data())
    log.info(s"Successfully set address ${buffer.data()}")
    val size              = length
    var i                 = 0
    val arrowArrayBuilder = ScalaFFIFactory.newStringArrowArrayBuilder
    arrowArrayBuilder.reserve(size)
    arrowArrayBuilder.reserveData(buffer.size())
    while (i < size) {
      arrowArrayBuilder.unsafeAppend(pointer, offset.get(i))
      pointer.addV(offset.get(i).toLong)
      i += 1
    }
    arrowArrayBuilder
  }

  def fillComplexArrowArrayBuilder[T: ClassTag](
      array: PrimitiveArray[T]
  ): StringArrowArrayBuilder = {
    val (buffer, offset) = serializeComplexArray(array)
    buffer.touch()
    val pointer =
      FFITypeFactory.getFactory(classOf[CCharPointer], "char").asInstanceOf[CCharPointer.Factory].create()
    pointer.setAddress(buffer.objAddress)
    log.info("Successfully set address")
    val size              = array.size()
    var i                 = 0
    val arrowArrayBuilder = ScalaFFIFactory.newStringArrowArrayBuilder
    arrowArrayBuilder.reserve(size)
    arrowArrayBuilder.reserveData(buffer.size())
    while (i < size) {
      arrowArrayBuilder.unsafeAppend(pointer, offset.get(i))
      pointer.addV(offset.get(i).toLong)
      i += 1
    }
    arrowArrayBuilder
  }

  def fillPrimitiveVector[T: ClassTag](
      array: Array[T],
      numThread: Int
  ): StdVector[T] = {
    val time0  = System.nanoTime()
    val size   = array.length
    val vector = ScalaFFIFactory.newVector[T]
    vector.resize(size)
    val threadArray = new Array[Thread](numThread)
    val atomic      = new AtomicInteger(0)
    for (i <- 0 until numThread) {
      threadArray(i) = new Thread() {
        override def run(): Unit = {
          var flag = true
          while (flag) {
            val begin = Math.min(atomic.getAndAdd(BATCH_SIZE), size)
            val end   = Math.min(begin + BATCH_SIZE, size)
            if (begin >= end) {
              flag = false
            } else {
              var i = begin
              while (i < end) {
                vector.set(i, array(i))
                i += 1
              }
            }
          }
        }
      }
      threadArray(i).start()
    }
    for (i <- 0 until numThread) {
      threadArray(i).join()
    }
    val time1 = System.nanoTime()
    log.info(
      s"Building primitive array size ${size} with num thread ${numThread} cost ${(time1 - time0) / 1000000}ms"
    )
    vector
  }

  def fillVertexTupleArrowArray[T: ClassTag](
      array: Array[T],
      activeVertices: ThreadSafeBitSet
  ): (FFIByteVector, FFIIntVector) = {
    val size                = array.length
    val ffiByteVectorOutput = new FFIByteVectorOutputStream()
    //    val output = new Output(ffiByteVectorOutput)
    val ffiOffset =
      FFIIntVectorFactory.INSTANCE.create().asInstanceOf[FFIIntVector]
    ffiOffset.resize(size)
    ffiOffset.touch()
    val objectOutputStream = new ObjectOutputStream(ffiByteVectorOutput)
    var i                  = activeVertices.nextSetBit(0)
    val limit              = size
    var prevBytesWritten   = 0
    var nullCount          = 0
    while (i < limit && i >= 0) {
      if (array(i) == null) {
        nullCount += 1
      }
      objectOutputStream.writeObject(array(i))
      ffiOffset.set(
        i,
        ffiByteVectorOutput.bytesWriten().toInt - prevBytesWritten
      )
      prevBytesWritten = ffiByteVectorOutput.bytesWriten().toInt
      i += 1
    }
    log.info(
      s"total size ${size} null count ${nullCount}, active ${activeVertices.cardinality()}"
    )
    //require(size == (nullCount + activeVertices.cardinality()))
    objectOutputStream.flush()
    ffiByteVectorOutput.finishSetting()
    val writenBytes = ffiByteVectorOutput.bytesWriten()
    log.info(
      s"write data array ${limit} of type ${GrapeUtils.getRuntimeClass[T].getName}, writen bytes ${writenBytes}"
    )
    (ffiByteVectorOutput.getVector, ffiOffset)
  }

  def getRuntimeClass[T: ClassTag] =
    implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]

  def fillPrimitiveVineyardArray[T: ClassTag](
      array: Array[T],
      vineyardBuilder: VineyardArrayBuilder[T],
      numThread: Int
  ): Unit = {
    val time0       = System.nanoTime()
    val size        = array.length
    val threadArray = new Array[Thread](numThread)
    val atomic      = new AtomicInteger(0)
    for (i <- 0 until numThread) {
      threadArray(i) = new Thread() {
        override def run(): Unit = {
          var flag = true
          while (flag) {
            val begin = Math.min(atomic.getAndAdd(BATCH_SIZE), size)
            val end   = Math.min(begin + BATCH_SIZE, size)
            if (begin >= end) {
              flag = false
            } else {
              var i = begin
              while (i < end) {
                vineyardBuilder.set(i, array(i))
                i += 1
              }
            }
          }
        }
      }
      threadArray(i).start()
    }
    for (i <- 0 until numThread) {
      threadArray(i).join()
    }
    val time1 = System.nanoTime()
    log.info(
      s"Building primitive array size ${size} with num thread ${numThread} cost ${(time1 - time0) / 1000000}ms"
    )
  }

  def serializeComplexArray[T: ClassTag](
      array: Array[T]
  ): (FFIByteVector, FFIIntVector) = { //,activeVertices : ThreadSafeBitSet
    val size                = array.length
    val ffiByteVectorOutput = new FFIByteVectorOutputStream()
    val ffiOffset =
      FFIIntVectorFactory.INSTANCE.create().asInstanceOf[FFIIntVector]
    ffiOffset.resize(size)
    ffiOffset.touch()
    var i                = 0
    val limit            = size
    var prevBytesWritten = 0
    var nullCount        = 0
    if (getRuntimeClass[T] == classOf[DoubleDouble]) {
      ffiByteVectorOutput.getVector.resize(size * 16)
      ffiByteVectorOutput.getVector.touch()
      val castedArray = array.asInstanceOf[Array[DoubleDouble]]
      while (i < limit) {
        val dd = castedArray(i)
        require(dd != null, s"pos ${i}/${limit} is null")
        ffiByteVectorOutput.writeDouble(dd.a)
        ffiByteVectorOutput.writeDouble(dd.b)
        ffiOffset.set(
          i,
          ffiByteVectorOutput.bytesWriten().toInt - prevBytesWritten
        )
        prevBytesWritten = ffiByteVectorOutput.bytesWriten().toInt
        i += 1
      }
    } else {
      val objectOutputStream = new ObjectOutputStream(ffiByteVectorOutput)
      while (i < limit && i >= 0) {
        if (array(i) == null) {
          nullCount += 1
        }
        objectOutputStream.writeObject(array(i))
        ffiOffset.set(
          i,
          ffiByteVectorOutput.bytesWriten().toInt - prevBytesWritten
        )
        prevBytesWritten = ffiByteVectorOutput.bytesWriten().toInt
        i += 1
      }
      objectOutputStream.flush()
    }

    ffiByteVectorOutput.finishSetting()
    val writenBytes = ffiByteVectorOutput.bytesWriten()
    log.info(
      s"write data array ${limit} of type ${GrapeUtils.getRuntimeClass[T].getName}, writen bytes ${writenBytes}"
    )
    (ffiByteVectorOutput.getVector, ffiOffset)
  }

  def serializeComplexArray[T: ClassTag](
      array: PrimitiveArray[T]
  ): (FFIByteVector, FFIIntVector) = { //,activeVertices : ThreadSafeBitSet
    val size                = array.size()
    val ffiByteVectorOutput = new FFIByteVectorOutputStream()
    val ffiOffset =
      FFIIntVectorFactory.INSTANCE.create().asInstanceOf[FFIIntVector]
    ffiOffset.resize(size)
    ffiOffset.touch()
    var i                = 0
    val limit            = size
    var prevBytesWritten = 0
    var nullCount        = 0
    if (getRuntimeClass[T] == classOf[DoubleDouble]) {
      ffiByteVectorOutput.getVector.resize(size * 16)
      ffiByteVectorOutput.getVector.touch()
      val castedArray = array.asInstanceOf[PrimitiveArray[DoubleDouble]]
      while (i < limit) {
        val dd = castedArray.get(i)
        require(dd != null, s"pos ${i}/${limit} is null")
        ffiByteVectorOutput.writeDouble(dd.a)
        ffiByteVectorOutput.writeDouble(dd.b)
        ffiOffset.set(
          i,
          ffiByteVectorOutput.bytesWriten().toInt - prevBytesWritten
        )
        prevBytesWritten = ffiByteVectorOutput.bytesWriten().toInt
        i += 1
      }
    } else {
      val objectOutputStream = new ObjectOutputStream(ffiByteVectorOutput)
      while (i < limit && i >= 0) {
        if (array.get(i) == null) {
          nullCount += 1
        }
        objectOutputStream.writeObject(array.get(i))
        ffiOffset.set(
          i,
          ffiByteVectorOutput.bytesWriten().toInt - prevBytesWritten
        )
        prevBytesWritten = ffiByteVectorOutput.bytesWriten().toInt
        i += 1
      }
      objectOutputStream.flush()
    }

    ffiByteVectorOutput.finishSetting()
    val writenBytes = ffiByteVectorOutput.bytesWriten()
    log.info(
      s"write data array ${limit} of type ${GrapeUtils.getRuntimeClass[T].getName}, writen bytes ${writenBytes}"
    )
    (ffiByteVectorOutput.getVector, ffiOffset)
  }

  /** Convert a quantity in bytes to a human-readable string such as "4.0 MiB".
    */
  def bytesToString(size: Long): String = bytesToString(BigInt(size))

  def bytesToString(size: BigInt): String = {
    val EiB = 1L << 60
    val PiB = 1L << 50
    val TiB = 1L << 40
    val GiB = 1L << 30
    val MiB = 1L << 20
    val KiB = 1L << 10

    if (size >= BigInt(1L << 11) * EiB) {
      // The number is too large, show it in scientific notation.
      BigDecimal(size, new MathContext(3, RoundingMode.HALF_UP))
        .toString() + " B"
    } else {
      val (value, unit) = {
        if (size >= 2 * EiB) {
          (BigDecimal(size) / EiB, "EiB")
        } else if (size >= 2 * PiB) {
          (BigDecimal(size) / PiB, "PiB")
        } else if (size >= 2 * TiB) {
          (BigDecimal(size) / TiB, "TiB")
        } else if (size >= 2 * GiB) {
          (BigDecimal(size) / GiB, "GiB")
        } else if (size >= 2 * MiB) {
          (BigDecimal(size) / MiB, "MiB")
        } else if (size >= 2 * KiB) {
          (BigDecimal(size) / KiB, "KiB")
        } else {
          (BigDecimal(size), "B")
        }
      }
      "%.1f %s".formatLocal(Locale.US, value, unit)
    }
  }

  /** Extract the distribution info, i.e. how many partition on each host.
    * @param rdd
    *   input rdd
    * @param numPartition
    *   num partitions.
    * @return
    */
  def extractHostInfo(
      rdd: RDD[(Int, _)],
      numPartition: Int
  ): (mutable.HashMap[String, ArrayBuffer[Int]], mutable.HashMap[Int, Int]) = {
    val array = rdd
      .mapPartitionsWithIndex((ind, iter) => {
        if (iter.hasNext) {
          val set = new mutable.BitSet()
          while (iter.hasNext) {
            val (ind, _) = iter.next()
            set.add(ind)
          }
          val spark_env = SparkEnv.get
          set.toIterator.map(a => (a, InetAddress.getLocalHost.getHostName, spark_env.executorId))
        } else Iterator.empty
      })
      .collect()
    log.info(
      s"num Partitions ${rdd.getNumPartitions}, collected size ${array.length} ele: ${array.mkString(",")}"
    )
    require(
      array.length == numPartition,
      s"result array neq num partitions ${array.length} , ${numPartition}"
    )
    val tmpMap           = new mutable.HashMap[String, ArrayBuffer[Int]]()
    val executorId2Times = new mutable.HashMap[Int, Int]()
    for (tuple <- array) {
      val executorId = tuple._3
      val host       = tuple._2
      val pid        = tuple._1
      if (!tmpMap.contains(host)) {
        tmpMap(host) = new ArrayBuffer[PartitionID]()
      }
      val executorIdInt = executorId.toInt
      executorId2Times.put(executorIdInt, executorId2Times.getOrElse(executorIdInt, 0) + 1)
      tmpMap(host).+=(pid)
    }
    val numPart = array.length
    val res     = new Array[Int](numPart) //pid to executor id
    val keys    = tmpMap.keys.iterator
    var i       = 0
    while (keys.hasNext) {
      val key    = keys.next()
      val values = tmpMap(key)
      for (pid <- values) {
        res(pid) = i
      }
      i += 1
    }
    (tmpMap, executorId2Times)
  }

  def getHostIdStrFromFragmentGroup(
      client: VineyardClient,
      groupId: Long
  ): String = {
    val fragmentGroupGetter = ScalaFFIFactory.newFragmentGroupGetter()
    val fragmentGroup       = fragmentGroupGetter.get(client, groupId).get()
    val fnum                = fragmentGroup.totalFragNum()
    log.info(s"group ${groupId} got totally ${fnum} fragments.")
    val locations = fragmentGroup.fragmentLocations()
    val fragments = fragmentGroup.fragments()
    val instances = new Array[Long](fnum)
    val fragIds   = new Array[Long](fnum)
    for (i <- 0 until fnum) {
      instances(i) = locations.get(i)
      fragIds(i) = fragments.get(i)
    }
    log.info(s"frag ids ${fragIds.mkString(",")}")
    log.info(s"instances ${instances.mkString(",")}")
    //get host info from instances.
    val clusterInfo =
      ScalaFFIFactory.newStdMap[Long, Json](signed = false) //uint64_t
    val status =
      client.clusterInfo(clusterInfo.asInstanceOf[StdMap[java.lang.Long, Json]])
    require(status.ok())
    log.info(s"got cluster info ${clusterInfo}")
    val jsons = instances
      .map(instance => clusterInfo.get(instance))
      .map(_.dump().toString)
    log.info(s"got jsons ${jsons.mkString(",")}")
    val hostNames = jsons.map(json => com.alibaba.fastjson.JSON.parseObject(json).getString("hostname"))
    fragIds.indices.map(i => hostNames(i) + ":" + fragIds(i)).mkString(",")
  }

  def hashSet2Vector(hashSet: OpenHashSet[Long]): StdVector[Long] = {
    val vector: StdVector[Long] = ScalaFFIFactory.newLongVector
    vector.resize(hashSet.size)

    val iter = hashSet.iterator
    var i    = 0
    while (i < hashSet.size) {
      vector.set(i, iter.next())
      i += 1
    }
    vector
  }

  def buildVectorWithDefaultValue[T: ClassTag](
      size: Int,
      vd: T
  ): StdVector[T] = {
    val vector: StdVector[T] = ScalaFFIFactory.newVector[T]
    vector.resize(size)

    var i = 0;
    while (i < size) {
      vector.set(i, vd)
      i += 1
    }
    vector
  }

  def buildVectorWithValueIterator[T: ClassTag](
      size: Int,
      vd: T
  ): StdVector[T] = {
    val vector: StdVector[T] = ScalaFFIFactory.newVector[T]
    vector.resize(size)

    var i = 0;
    while (i < size) {
      vector.set(i, vd)
      i += 1
    }
    vector
  }

  @throws[IOException]
  @throws[ClassNotFoundException]
  def readComplexArray[T: ClassTag](oldArray: StringTypedArray): Array[T] = {
    val vector   = new FakeFFIByteVector(oldArray.getRawData, oldArray.getRawDataLength)
    val ffiInput = new FakeFFIByteVectorInputStream(vector)
    val len      = oldArray.getLength
    log.info(s"reading ${len} objects from array of bytes ${oldArray.getLength}")
    if (GrapeUtils.getRuntimeClass[T] == classOf[DoubleDouble]) {
      val newArray = new Array[DoubleDouble](len.toInt)
      var i        = 0
      while (i < len) {
        val a = ffiInput.readDouble
        val b = ffiInput.readDouble
        newArray(i) = new DoubleDouble(a, b)
        i += 1
      }
      newArray.asInstanceOf[Array[T]]
    } else {
      val newArray          = new Array[T](len.toInt)
      val objectInputStream = new ObjectInputStream(ffiInput)
      var i                 = 0
      while (i < len) {
        val obj = objectInputStream.readObject.asInstanceOf[T]
        newArray(i) = obj
        i += 1
      }
      newArray
    }
  }
}
