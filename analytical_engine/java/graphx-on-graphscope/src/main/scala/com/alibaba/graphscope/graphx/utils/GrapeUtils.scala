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

import com.alibaba.fastffi.impl.CXXStdString
import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder
import com.alibaba.graphscope.graphx._
import com.alibaba.graphscope.graphx.store.impl.OffHeapEdgeDataStore
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream
import com.alibaba.graphscope.stdcxx.{FFIByteVector, FFIIntVector, FFIIntVectorFactory, StdMap, StdVector}
import com.alibaba.graphscope.utils.ThreadSafeBitSet
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.graphx.PartitionID
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import java.io.ObjectOutputStream
import java.lang.reflect.Method
import java.math.{MathContext, RoundingMode}
import java.net.{InetAddress, UnknownHostException}
import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object GrapeUtils extends Logging{
  val BATCH_SIZE = 4096

  def class2Int(value: Class[_]): Int = {
    if (value.equals(classOf[java.lang.Long]) || value.equals(classOf[Long])) {
      4
    }
    else if (value.equals(classOf[java.lang.Integer]) || value.equals(classOf[Int])) {
      2
    }
    else if (value.equals(classOf[java.lang.Double]) || value.eq(classOf[Double])) {
      7
    }
    else throw new IllegalArgumentException(s"unexpected class ${value}")
  }
  def bytesForType[VD: ClassTag](value: Class[VD]): Int ={
    if (value.equals(classOf[Long]) || value.equals(classOf[Double])) 8
    else if (value.eq(classOf[Int]) || value.equals(classOf[Float])) 4
    else throw new IllegalStateException("Unrecognized : " + value.getName)
  }

  def classToStr(value : Class[_], signed : Boolean = true) : String = {
    if (value.equals(classOf[java.lang.Long]) || value.equals(classOf[Long])) {
      if (signed) "int64_t"
      else "uint64_t"
    }
    else if (value.equals(classOf[java.lang.Integer]) || value.equals(classOf[Int])) {
      if (signed) "int32_t"
      else "uint32_t"
    }
    else if (value.equals(classOf[java.lang.Double]) || value.eq(classOf[Double])) {
      "double"
    }
    else if (value.equals(classOf[Json])){
      "vineyard::json"
    }
    else {
      "std::string"
    }
  }

  def getMethodFromClass[T](clz : Class[T], name : String ,paramClasses : Class[_]) : Method = {
    val method = clz.getDeclaredMethod(name, paramClasses)
    method.setAccessible(true)
    require(method != null, "can not find method: " + name)
    method
  }

  def generateForeignFragName[VD: ClassTag, ED : ClassTag](vdClass : Class[VD], edClass : Class[ED]): String ={
    new StringBuilder().+("gs::ArrowProjectedFragment<int64_t,uint64_t").+(classToStr(vdClass)).+(",").+(classToStr(edClass)).+(">")
  }

  def scalaClass2JavaClass[T: ClassTag](vdClass : Class[T]) : Class[_] = {
    if (vdClass.equals(classOf[Int])) {
      classOf[Integer];
    }
    else if (vdClass.equals(classOf[Long])) {
      classOf[java.lang.Long]
    }
    else if (vdClass.equals(classOf[Double])) {
      classOf[java.lang.Double]
    }
    else {
      throw new IllegalStateException("transform failed for " + vdClass.getName);
    }
  }

  def getRuntimeClass[T: ClassTag] = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]

  def isPrimitive[T : ClassTag] : Boolean = {
    val clz = getRuntimeClass[T]
    clz.equals(classOf[Double]) || clz.equals(classOf[Long]) || clz.equals(classOf[Int]) || clz.equals(classOf[Float])
  }

  @throws[UnknownHostException]
  def getSelfHostName = InetAddress.getLocalHost.getHostName

  def dedup(files: Array[String]): Array[String] = {
    val set = files.toSet
    set.toArray
  }

  /**
   * used to transform offset base edata store to eids based store.
   * @tparam T data type
   */
  def rearrangeArrayWithIndex[T : ClassTag](array : PrimitiveArray[T], index : PrimitiveArray[Long]) : PrimitiveArray[T] = {
    val len = array.size()
    require(index.size() == len, s"array size ${len} neq eids array ${index.size()}")
    val newArray = PrimitiveArray.create(getRuntimeClass[T], len).asInstanceOf[PrimitiveArray[T]]
    var i = 0
    while (i < len){
      newArray.set(i, array.get(index.get(i)))
      i += 1
    }
    newArray
  }

  def fillPrimitiveArrowArrayBuilder[T : ClassTag](array: Array[T]) : ArrowArrayBuilder[T] = {
    val size = array.length
    var i = 0
    val arrowArrayBuilder = ScalaFFIFactory.newArrowArrayBuilder[T](GrapeUtils.getRuntimeClass[T].asInstanceOf[Class[T]])
    arrowArrayBuilder.reserve(size)
    while (i < size) {
      arrowArrayBuilder.unsafeAppend(array(i))
      i += 1
    }
    arrowArrayBuilder
  }
  def fillPrimitiveVector[T : ClassTag](array: Array[T], numThread : Int) : StdVector[T] = {
    val time0 = System.nanoTime()
    val size = array.length
    val vector = ScalaFFIFactory.newVector[T]
    vector.resize(size)
    val threadArray = new Array[Thread](numThread)
    val atomic = new AtomicInteger(0)
    for (i <- 0 until numThread){
      threadArray(i) = new Thread(){
        override def run(): Unit ={
          var flag = true
          while (flag){
            val begin = Math.min(atomic.getAndAdd(BATCH_SIZE), size)
            val end = Math.min(begin + BATCH_SIZE, size)
            if (begin >= end){
              flag = false
            }
            else {
              var i = begin
              while (i < end){
                vector.set(i, array(i))
                i += 1
              }
            }
          }
        }
      }
      threadArray(i).start()
    }
    for (i <- 0 until numThread){
      threadArray(i).join()
    }
    val time1 = System.nanoTime()
    log.info(s"Building primitive array size ${size} with num thread ${numThread} cost ${(time1 - time0)/1000000}ms")
    vector
  }
  def fillPrimitiveVineyardArray[T : ClassTag](array: Array[T], vineyardBuilder : VineyardArrayBuilder[T], numThread : Int) : Unit = {
    val time0 = System.nanoTime()
    val size = array.length
    val threadArray = new Array[Thread](numThread)
    val atomic = new AtomicInteger(0)
    for (i <- 0 until numThread){
      threadArray(i) = new Thread(){
        override def run(): Unit ={
          var flag = true
          while (flag){
            val begin = Math.min(atomic.getAndAdd(BATCH_SIZE), size)
            val end = Math.min(begin + BATCH_SIZE, size)
            if (begin >= end){
              flag = false
            }
            else {
              var i = begin
              while (i < end){
                vineyardBuilder.set(i, array(i))
                i += 1
              }
            }
          }
        }
      }
      threadArray(i).start()
    }
    for (i <- 0 until numThread){
      threadArray(i).join()
    }
    val time1 = System.nanoTime()
    log.info(s"Building primitive array size ${size} with num thread ${numThread} cost ${(time1 - time0)/1000000}ms")
  }

  def fillVertexStringArrowArray[T : ClassTag](array: Array[T]) : (FFIByteVector, FFIIntVector) = {//,activeVertices : ThreadSafeBitSet
    val size = array.length
    val ffiByteVectorOutput = new FFIByteVectorOutputStream()
    val ffiOffset = FFIIntVectorFactory.INSTANCE.create().asInstanceOf[FFIIntVector]
    ffiOffset.resize(size)
    ffiOffset.touch()
    var i = 0
    val limit = size
    var prevBytesWritten = 0
    var nullCount = 0
    if (getRuntimeClass[T] == classOf[DoubleDouble]){
      ffiByteVectorOutput.getVector.resize(size * 16)
      ffiByteVectorOutput.getVector.touch()
      val castedArray = array.asInstanceOf[Array[DoubleDouble]]
      while (i < limit){
        val dd = castedArray(i)
        require(dd != null, s"pos ${i}/${limit} is null")
        ffiByteVectorOutput.writeDouble(dd.a)
        ffiByteVectorOutput.writeDouble(dd.b)
        ffiOffset.set(i, ffiByteVectorOutput.bytesWriten().toInt - prevBytesWritten)
        prevBytesWritten = ffiByteVectorOutput.bytesWriten().toInt
        i += 1
      }
    }
    else {
      val objectOutputStream = new ObjectOutputStream(ffiByteVectorOutput)
      while (i < limit && i >= 0) {
        if (array(i) == null) {
          nullCount += 1
        }
        objectOutputStream.writeObject(array(i))
        ffiOffset.set(i, ffiByteVectorOutput.bytesWriten().toInt - prevBytesWritten)
        prevBytesWritten = ffiByteVectorOutput.bytesWriten().toInt
        i += 1
      }
      objectOutputStream.flush()
    }

    ffiByteVectorOutput.finishSetting()
    val writenBytes = ffiByteVectorOutput.bytesWriten()
    log.info(s"write data array ${limit} of type ${GrapeUtils.getRuntimeClass[T].getName}, writen bytes ${writenBytes}")
    (ffiByteVectorOutput.getVector,ffiOffset)
  }
  def fillVertexTupleArrowArray[T : ClassTag](array: Array[T],activeVertices : ThreadSafeBitSet) : (FFIByteVector, FFIIntVector) = {
    val size = array.length
    val ffiByteVectorOutput = new FFIByteVectorOutputStream()
    //    val output = new Output(ffiByteVectorOutput)
    val ffiOffset = FFIIntVectorFactory.INSTANCE.create().asInstanceOf[FFIIntVector]
    ffiOffset.resize(size)
    ffiOffset.touch()
    val objectOutputStream = new ObjectOutputStream(ffiByteVectorOutput)
    var i = activeVertices.nextSetBit(0)
    val limit = size
    var prevBytesWritten = 0
    var nullCount = 0
    while (i < limit && i >= 0){
      if (array(i) == null){
        nullCount +=1
      }
      objectOutputStream.writeObject(array(i))
      ffiOffset.set(i, ffiByteVectorOutput.bytesWriten().toInt - prevBytesWritten)
      prevBytesWritten = ffiByteVectorOutput.bytesWriten().toInt
      i += 1
    }
    log.info(s"total size ${size} null count ${nullCount}, active ${activeVertices.cardinality()}")
    //require(size == (nullCount + activeVertices.cardinality()))
    objectOutputStream.flush()
    ffiByteVectorOutput.finishSetting()
    val writenBytes = ffiByteVectorOutput.bytesWriten()
    log.info(s"write data array ${limit} of type ${GrapeUtils.getRuntimeClass[T].getName}, writen bytes ${writenBytes}")
    (ffiByteVectorOutput.getVector,ffiOffset)
  }

  def fillEdgeStringArrowArray[T : ClassTag](array: Array[T]) : (FFIByteVector, FFIIntVector) = {
    val size = array.length
    val ffiByteVectorOutput = new FFIByteVectorOutputStream()
    val ffiOffset = FFIIntVectorFactory.INSTANCE.create().asInstanceOf[FFIIntVector]
    ffiOffset.resize(size)
    ffiOffset.touch()
    val objectOutputStream = new ObjectOutputStream(ffiByteVectorOutput)
    val limit = size
    var i = 0
    var prevBytesWritten = 0
    var nullCount = 0
    while (i < limit && i >= 0){
      if (array(i) == null){
        nullCount +=1
      }
      objectOutputStream.writeObject(array(i))
      ffiOffset.set(i, ffiByteVectorOutput.bytesWriten().toInt - prevBytesWritten)
      prevBytesWritten = ffiByteVectorOutput.bytesWriten().toInt
      i += 1
    }
    objectOutputStream.flush()
    ffiByteVectorOutput.finishSetting()
    val writenBytes = ffiByteVectorOutput.bytesWriten()
    log.info(s"write data array ${limit} of type ${GrapeUtils.getRuntimeClass[T].getName}, writen bytes ${writenBytes}")
    (ffiByteVectorOutput.getVector,ffiOffset)
  }


  def array2PrimitiveVertexData[T: ClassTag](array : Array[T], client : VineyardClient) : VertexData[Long,T] = {
    val newVdataBuilder = ScalaFFIFactory.newVertexDataBuilder[T](client,array.length,null.asInstanceOf[T])
    val valuesBuilder = newVdataBuilder.getArrayBuilder
    var i = 0
    while (i < array.length){
      valuesBuilder.set(i, array(i))
      i += 1
    }
    newVdataBuilder.seal(client).get()
  }

  def array2PrimitiveEdgeData[T: ClassTag](array : Array[T], client : VineyardClient, numThread : Int) : EdgeData[Long,T] = {
    val newEdataBuilder = ScalaFFIFactory.newEdgeDataBuilder[T](client,array.size)
    fillPrimitiveVineyardArray(array,newEdataBuilder.getArrayBuilder,numThread)
    newEdataBuilder.seal(client).get()
  }

  def array2StringVertexData[T : ClassTag](array: Array[T],
                                           client: VineyardClient) : StringVertexData[Long,CXXStdString] = {
    val (ffiByteVector,ffiIntVector) = fillVertexStringArrowArray(array)
    val newVdataBuilder = ScalaFFIFactory.newStringVertexDataBuilder()
    newVdataBuilder.init(array.length, ffiByteVector, ffiIntVector)
    newVdataBuilder.seal(client).get()
  }

  def bitSet2longs(bitSetWithOffset: BitSetWithOffset) : ArrowArrayBuilder[Long] = {
    val longVector = ScalaFFIFactory.newSignedLongArrayBuilder()
    val words = bitSetWithOffset.bitset.words
    longVector.reserve(words.length)
    var i = 0
    while (i < words.length){
      longVector.unsafeAppend(words(i))
      i += 1
    }
    longVector
  }
  def bitSet2longs(bitSetWithOffset: ThreadSafeBitSet) : ArrowArrayBuilder[Long] = {
    val longVector = ScalaFFIFactory.newSignedLongArrayBuilder()
    val words = bitSetWithOffset.getWords
    longVector.reserve(words.length)
    var i = 0
    while (i < words.length){
      longVector.unsafeAppend(words(i))
      i += 1
    }
    longVector
  }

  def array2StringEdgeData[T : ClassTag](array: Array[T],client: VineyardClient) : StringEdgeData[Long,CXXStdString] = {
    val (ffiByteVector,ffiIntVector) = fillEdgeStringArrowArray(array)
    val newEdataBuilder = ScalaFFIFactory.newStringEdgeDataBuilder()
    newEdataBuilder.init(array.length, ffiByteVector, ffiIntVector)
    newEdataBuilder.seal(client).get()
  }

  def buildPrimitiveEdgeData[T : ClassTag](edgeStore : OffHeapEdgeDataStore[T], client : VineyardClient, localNum : Int): EdgeData[Long,T]  = {
    edgeStore.edataBuilder.seal(client).get()
  }

  /**
   * Convert a quantity in bytes to a human-readable string such as "4.0 MiB".
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
      BigDecimal(size, new MathContext(3, RoundingMode.HALF_UP)).toString() + " B"
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

  def extractHostInfo[VD : ClassTag, ED : ClassTag](rdd : RDD[(Int,_)], numPartition : Int) : (Array[String], Array[Int]) = {
    val array = rdd.mapPartitionsWithIndex((ind,iter) => {
      if (iter.hasNext){
        val set = new mutable.BitSet()
        while (iter.hasNext){
          val (ind,_) = iter.next()
          set.add(ind)
        }
        set.toIterator.map(a => (a,InetAddress.getLocalHost.getHostName))
      }
      else Iterator.empty
    }).collect()
    log.info(s"num Partitions ${rdd.getNumPartitions}, collected size ${array.length} ele: ${array.mkString(",")}")
    require(array.length == numPartition, s"result array neq num partitions ${array.length} , ${numPartition}")
    val tmpMap = new mutable.HashMap[String,ArrayBuffer[Int]]()
    for (tuple <- array){
      val host = tuple._2
      val pid = tuple._1
      if (!tmpMap.contains(host)) {
        tmpMap(host) = new ArrayBuffer[PartitionID]()
      }
      tmpMap(host).+=(pid)
    }
    val numPart = array.length
    val res = new Array[Int](numPart)
    val keys = tmpMap.keys.iterator
    var i = 0
    while (keys.hasNext){
      val key = keys.next()
      val values = tmpMap(key)
      for (pid <- values){
        res(pid) = i
      }
      i += 1
    }
    (tmpMap.keys.toArray,res)
  }

  def getHostIdStrFromFragmentGroup(client: VineyardClient,groupId : Long) : String = {
    val fragmentGroupGetter = ScalaFFIFactory.newFragmentGroupGetter()
    val fragmentGroup = fragmentGroupGetter.get(client, groupId).get()
    val fnum = fragmentGroup.totalFragNum()
    log.info(s"group ${groupId} got totally ${fnum} fragments.")
    val locations = fragmentGroup.fragmentLocations()
    val fragments = fragmentGroup.fragments()
    val instances = new Array[Long](fnum)
    val fragIds = new Array[Long](fnum)
    for (i <- 0 until fnum){
      instances(i) = locations.get(i)
      fragIds(i) = fragments.get(i)
    }
    log.info(s"frag ids ${fragIds.mkString(",")}")
    log.info(s"instances ${instances.mkString(",")}")
    //get host info from instances.
    val clusterInfo = ScalaFFIFactory.newStdMap[Long,Json](signed = false) //uint64_t
    val status = client.clusterInfo(clusterInfo.asInstanceOf[StdMap[java.lang.Long,Json]])
    require(status.ok())
    log.info(s"got cluster info ${clusterInfo}")
    val jsons = instances.map(instance => clusterInfo.get(instance)).map(_.dump().toString)
    log.info(s"got jsons ${jsons.mkString(",")}")
    val hostNames = jsons.map(json => com.alibaba.fastjson.JSON.parseObject(json).getString("hostname"))
    fragIds.indices.map(i => hostNames(i) + ":" + fragIds(i)).mkString(",")
  }
}
