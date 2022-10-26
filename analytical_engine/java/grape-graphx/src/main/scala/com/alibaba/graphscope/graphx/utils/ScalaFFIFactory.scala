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
import com.alibaba.fastffi.impl.CXXStdString
import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor
import com.alibaba.graphscope.fragment.{ArrowFragmentGroupGetter, ArrowProjectedFragmentGetter, ArrowProjectedFragmentMapper, IFragment}
import com.alibaba.graphscope.graphx._
import com.alibaba.graphscope.stdcxx.{StdMap, StdVector}
import org.apache.spark.internal.Logging

import java.util.HashMap
import scala.reflect.ClassTag

object ScalaFFIFactory extends Logging{
  private val arrowArrayBuilderMap = new HashMap[String, ArrowArrayBuilder.Factory[_]]
  val clientFactory : VineyardClient.Factory = FFITypeFactory.getFactory(classOf[VineyardClient],"vineyard::Client").asInstanceOf[VineyardClient.Factory]
  def newLocalVertexMapBuilder(client : VineyardClient, innerOids : ArrowArrayBuilder[Long],
                               outerOids : ArrowArrayBuilder[Long],
                               pids : ArrowArrayBuilder[Int]): BasicLocalVertexMapBuilder[Long,Long] = synchronized{
     val localVertexMapBuilderFactory = FFITypeFactory.getFactory(classOf[BasicLocalVertexMapBuilder[Long,Long]], "gs::BasicLocalVertexMapBuilder<int64_t,uint64_t>").asInstanceOf[BasicLocalVertexMapBuilder.Factory[Long,Long]]
    localVertexMapBuilderFactory.create(client, innerOids, outerOids,pids.asInstanceOf[ArrowArrayBuilder[java.lang.Integer]])
  }

  def getArrowArrayBuilderFactory(foreignTypeName: String): ArrowArrayBuilder.Factory[_] = synchronized{
    if (!arrowArrayBuilderMap.containsKey(foreignTypeName)) {
      synchronized{
        if (!arrowArrayBuilderMap.containsKey(foreignTypeName)){
          arrowArrayBuilderMap.put(foreignTypeName, FFITypeFactory.getFactory(classOf[ArrowArrayBuilder[_]], foreignTypeName))
        }
      }
    }
    arrowArrayBuilderMap.get(foreignTypeName)
  }

  def newArrowArrayBuilder[T : ClassTag](clz: Class[T]): ArrowArrayBuilder[T] = synchronized{
    if (clz.equals(classOf[java.lang.Long]) || clz.equals(classOf[Long])){
      getArrowArrayBuilderFactory("gs::ArrowArrayBuilder<int64_t>").create().asInstanceOf[ArrowArrayBuilder[T]]
    }
    else if (clz.equals(classOf[java.lang.Double]) || clz.equals(classOf[Double])){
      getArrowArrayBuilderFactory("gs::ArrowArrayBuilder<double>").create().asInstanceOf[ArrowArrayBuilder[T]]
    }
    else if (clz.equals(classOf[Integer]) || clz.equals(classOf[Int])){
      getArrowArrayBuilderFactory("gs::ArrowArrayBuilder<int32_t>").create().asInstanceOf[ArrowArrayBuilder[T]]
    }
    else throw new IllegalStateException("Not recognized " + clz.getName)
  }

  def newLongVector : StdVector[Long] = synchronized{
    val factory = FFITypeFactory.getFactory(classOf[StdVector[Long]],
      "std::vector<int64_t>").asInstanceOf[StdVector.Factory[Long]]
    factory.create()
  }
  def newIntVector : StdVector[Int] = synchronized{
    val factory = FFITypeFactory.getFactory(classOf[StdVector[Int]],
      "std::vector<int32_t>").asInstanceOf[StdVector.Factory[Int]]
    factory.create()
  }

  def newDoubleVector : StdVector[Double] = synchronized{
    val factory = FFITypeFactory.getFactory(classOf[StdVector[Double]],
      "std::vector<double>").asInstanceOf[StdVector.Factory[Double]]
    factory.create()
  }

  def newVector[T : ClassTag] : StdVector[T] = synchronized{
    if (GrapeUtils.getRuntimeClass[T].equals(classOf[Long])) newLongVector.asInstanceOf[StdVector[T]]
    else if (GrapeUtils.getRuntimeClass[T].equals(classOf[Int])) newIntVector.asInstanceOf[StdVector[T]]
    else if (GrapeUtils.getRuntimeClass[T].equals(classOf[Double])) newDoubleVector.asInstanceOf[StdVector[T]]
    else throw new IllegalArgumentException(s"unsupported ${GrapeUtils.getRuntimeClass[T].getName}")
  }

  def newUnsignedLongArrayBuilder(): ArrowArrayBuilder[Long] = synchronized{
    getArrowArrayBuilderFactory("gs::ArrowArrayBuilder<uint64_t>").create().asInstanceOf[ArrowArrayBuilder[Long]]
  }
  def newSignedLongArrayBuilder(): ArrowArrayBuilder[Long] = synchronized{
    getArrowArrayBuilderFactory("gs::ArrowArrayBuilder<int64_t>").create().asInstanceOf[ArrowArrayBuilder[Long]]
  }
  def newSignedIntArrayBuilder(): ArrowArrayBuilder[Int] = synchronized{
    getArrowArrayBuilderFactory("gs::ArrowArrayBuilder<int32_t>").create().asInstanceOf[ArrowArrayBuilder[Int]]
  }

  def newVertexMapGetter() : GraphXVertexMapGetter[Long,Long] = synchronized {
    val factory = FFITypeFactory.getFactory(classOf[GraphXVertexMapGetter[Long,Long]], "gs::GraphXVertexMapGetter<int64_t,uint64_t>").asInstanceOf[GraphXVertexMapGetter.Factory[Long,Long]]
    factory.create()
  }

  def newVertexDataGetter[VD : ClassTag] : VertexDataGetter[Long,VD] = synchronized {
    val factory = FFITypeFactory.getFactory(classOf[VertexDataGetter[Long,VD]],
      "gs::VertexDataGetter<uint64_t," + GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[VD]) + ">")
      .asInstanceOf[VertexDataGetter.Factory[Long,VD]]
    factory.create()
  }
  def newStringVertexDataGetter: StringVertexDataGetter[Long,CXXStdString] = synchronized {
    val factory = FFITypeFactory.getFactory(classOf[StringVertexDataGetter[Long,CXXStdString]],
      "gs::VertexDataGetter<uint64_t,std::string>")
      .asInstanceOf[StringVertexDataGetter.Factory[Long,CXXStdString]]
    factory.create()
  }

  def newGraphXCSRBuilder(client : VineyardClient) : BasicGraphXCSRBuilder[Long,Long] = synchronized {
    val factory = FFITypeFactory.getFactory(classOf[BasicGraphXCSRBuilder[Long,Long]],
      "gs::BasicGraphXCSRBuilder<int64_t,uint64_t>").asInstanceOf[BasicGraphXCSRBuilder.Factory[Long,Long]]
    factory.create(client)
  }

  def newVertexDataBuilder[VD: ClassTag](client: VineyardClient, fragVnums : Int, defaultVD : VD = null) : VertexDataBuilder[Long,VD] = synchronized {
    val factory = FFITypeFactory.getFactory(classOf[VertexDataBuilder[Long,VD]],
      "gs::VertexDataBuilder<uint64_t," + GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[VD]) +">").asInstanceOf[VertexDataBuilder.Factory[Long,VD]]
    if (defaultVD == null) {factory.create(client,fragVnums)}
    else factory.create(client,fragVnums,defaultVD)
  }

  def newEdgeDataBuilder[VD: ClassTag](client : VineyardClient, size : Int) : EdgeDataBuilder[Long,VD] = synchronized{
    val factory = FFITypeFactory.getFactory(classOf[EdgeDataBuilder[Long,VD]],
      "gs::EdgeDataBuilder<uint64_t," + GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[VD]) +">").asInstanceOf[EdgeDataBuilder.Factory[Long,VD]]
    factory.create(client,size)
  }

  def newVineyardArrayBuilder[T : ClassTag](client : VineyardClient, size : Int) : VineyardArrayBuilder[T] = {
    val factory = FFITypeFactory.getFactory(classOf[VineyardArrayBuilder[T]],
      "vineyard::ArrayBuilder<" + GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[T]) +">").asInstanceOf[VineyardArrayBuilder.Factory[T]]
    factory.create(client,size)
  }

  def newStringVertexDataBuilder() : StringVertexDataBuilder[Long,CXXStdString] = synchronized{
    val factory = FFITypeFactory.getFactory(classOf[StringVertexDataBuilder[Long,CXXStdString]],
      "gs::VertexDataBuilder<uint64_t,std::string>").asInstanceOf[StringVertexDataBuilder.Factory[Long,CXXStdString]]
    factory.create()
  }

  def newStringEdgeDataBuilder() : StringEdgeDataBuilder[Long,CXXStdString] = synchronized {
    val factory = FFITypeFactory.getFactory(classOf[StringEdgeDataBuilder[Long,CXXStdString]],
      "gs::EdgeDataBuilder<uint64_t,std::string>").asInstanceOf[StringEdgeDataBuilder.Factory[Long,CXXStdString]]
    factory.create()
  }

  def newVineyardClient() : VineyardClient = synchronized{
    synchronized{
      clientFactory.create()
    }
  }

  def newGraphXFragmentBuilder[VD : ClassTag,ED : ClassTag](client : VineyardClient, vmId : Long, csrId : Long, vdId : Long, edId : Long) : GraphXFragmentBuilder[Long,Long,VD,ED] = synchronized{
    require(GrapeUtils.isPrimitive[VD] && GrapeUtils.isPrimitive[ED])
    val factory = FFITypeFactory.getFactory(classOf[GraphXFragmentBuilder[Long,Long,VD,ED]], "gs::GraphXFragmentBuilder<int64_t,uint64_t," +
        GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[VD]) + "," + GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[ED]) + ">").asInstanceOf[GraphXFragmentBuilder.Factory[Long,Long,VD,ED]]
    factory.create(client, vmId, csrId,vdId,edId)
  }

  def newGraphXFragmentBuilder[VD : ClassTag,ED : ClassTag](client : VineyardClient, vm : GraphXVertexMap[Long,Long], csr : GraphXCSR[Long], vertexData : VertexData[Long,VD], edata : EdgeData[Long,ED]) : GraphXFragmentBuilder[Long,Long,VD,ED] = synchronized{
    require(GrapeUtils.isPrimitive[VD] && GrapeUtils.isPrimitive[ED])
    val factory = FFITypeFactory.getFactory(classOf[GraphXFragmentBuilder[Long,Long,VD,ED]], "gs::GraphXFragmentBuilder<int64_t,uint64_t," +
      GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[VD]) + "," + GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[ED]) + ">").asInstanceOf[GraphXFragmentBuilder.Factory[Long,Long,VD,ED]]
    factory.create(client, vm, csr,vertexData,edata)
  }

  def newGraphXStringVDFragmentBuiler[ED : ClassTag](client : VineyardClient, vm : GraphXVertexMap[Long,Long], csr : GraphXCSR[Long], vertexData : StringVertexData[Long,CXXStdString],edata : EdgeData[Long,ED]) : StringVDGraphXFragmentBuilder[Long,Long,CXXStdString,ED] = synchronized{
    require(GrapeUtils.isPrimitive[ED])
    val factory = FFITypeFactory.getFactory(classOf[StringVDGraphXFragmentBuilder[Long,Long,CXXStdString,ED]], "gs::GraphXFragmentBuilder<int64_t,uint64_t,std::string," +
        GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[ED]) + ">").asInstanceOf[StringVDGraphXFragmentBuilder.Factory[Long,Long,CXXStdString,ED]]
    factory.create(client, vm,csr,vertexData,edata)
  }
  def newGraphXStringEDFragmentBuilder[VD : ClassTag](client : VineyardClient, vm : GraphXVertexMap[Long,Long], csr : GraphXCSR[Long], vertexData : VertexData[Long,VD],edata : StringEdgeData[Long,CXXStdString]) : StringEDGraphXFragmentBuilder[Long,Long,VD,CXXStdString] = synchronized{
    require(GrapeUtils.isPrimitive[VD])
    val factory = FFITypeFactory.getFactory(classOf[StringEDGraphXFragmentBuilder[Long,Long,VD,CXXStdString]], "gs::GraphXFragmentBuilder<int64_t,uint64_t," +
        GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[VD]) + ",std::string>").asInstanceOf[StringEDGraphXFragmentBuilder.Factory[Long,Long,VD,CXXStdString]]
    factory.create(client, vm,csr,vertexData,edata)
  }

  def newGraphXStringVEDFragmentBuilder(client : VineyardClient, vm : GraphXVertexMap[Long,Long], csr : GraphXCSR[Long], vertexData: StringVertexData[Long,CXXStdString], edgeData: StringEdgeData[Long,CXXStdString]) : StringVEDGraphXFragmentBuilder[Long,Long,CXXStdString,CXXStdString] = synchronized{
    val factory = FFITypeFactory.getFactory(classOf[StringVEDGraphXFragmentBuilder[Long,Long,CXXStdString,CXXStdString]], "gs::GraphXFragmentBuilder<int64_t,uint64_t,std::string,std::string>").asInstanceOf[StringVEDGraphXFragmentBuilder.Factory[Long,Long,CXXStdString,CXXStdString]]
    factory.create(client, vm,csr,vertexData,edgeData)
  }

  def newProjectedFragmentMapper[NEW_VD : ClassTag, NEW_ED : ClassTag, OLD_VD, OLD_ED](oldVdClz : Class[OLD_VD], oldEDClz : Class[OLD_ED]) : ArrowProjectedFragmentMapper[Long,Long,OLD_VD,NEW_VD,OLD_ED,NEW_ED] = synchronized{
    val factory = FFITypeFactory.getFactory(classOf[ArrowProjectedFragmentMapper[Long,Long,OLD_VD,NEW_VD,OLD_ED,NEW_ED]],
    "gs::ArrowProjectedFragmentMapper<int64_t,uint64_t," +GrapeUtils.classToStr(oldVdClz) + "," + GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[NEW_VD]) + ","+ GrapeUtils.classToStr(oldEDClz) + ","
      + GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[NEW_ED]) + ">").asInstanceOf[ArrowProjectedFragmentMapper.Factory[Long,Long,OLD_VD,NEW_VD,OLD_ED, NEW_ED]]
    factory.create()
  }

  def getFragment[VD : ClassTag,ED : ClassTag](client : VineyardClient, objectID : Long, fragStr : String) : IFragment[Long,Long,VD,ED]= synchronized{
    if (fragStr.startsWith("gs::ArrowFragment")){
      throw new IllegalStateException("Not implemented now")
    }
    else if (fragStr.startsWith("gs::ArrowProjectedFragment")){
      log.info(s"Getting fragment for ${fragStr}, ${objectID}")
      val getterStr = fragName2FragGetterName(fragStr)
      val factory = FFITypeFactory.getFactory(classOf[ArrowProjectedFragmentGetter[Long,Long,VD,ED]], getterStr).asInstanceOf[ArrowProjectedFragmentGetter.Factory[Long,Long,VD,ED]]
      val fragmentGetter = factory.create()
      val res = fragmentGetter.get(client, objectID)
      new ArrowProjectedAdaptor[Long,Long,VD,ED](res.get())
    }
    else {
      throw new IllegalStateException(s"Not recognized frag str ${fragStr}")
    }
  }
  /** transform the frag name to frag getter name. */
  def fragName2FragGetterName(str : String) : String = synchronized{
    if (str.contains("ArrowProjectedFragment")){
      str.replace("ArrowProjectedFragment", "ArrowProjectedFragmentGetter")
    }
    else if (str.contains("ArrowFragment")){
      str.replace("ArrowFragment", "ArrowFragmentGetter")
    }
    else {
      throw new IllegalStateException(s"Not recognized ${str}")
    }
  }

  def newFragmentGroupGetter() : ArrowFragmentGroupGetter = {
    val factory = FFITypeFactory.getFactory(classOf[ArrowFragmentGroupGetter],
      "gs::ArrowFragmentGroupGetter").asInstanceOf[ArrowFragmentGroupGetter.Factory]
    factory.create()
  }

  def newStdMap[K : ClassTag, V : ClassTag](signed : Boolean = true) : StdMap[K,V] = {
    val factory = FFITypeFactory.getFactory(classOf[StdMap[K,V]],
      "std::map<" + GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[K], signed) + "," +
    GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[V], signed) + ">").asInstanceOf[StdMap.Factory[K,V]]
    factory.create()
  }

}
