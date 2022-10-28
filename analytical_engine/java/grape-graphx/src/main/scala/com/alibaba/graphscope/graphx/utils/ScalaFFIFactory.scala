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
import com.alibaba.graphscope.arrow.array.{ArrowArrayBuilder, ArrowStringArrayBuilder}
import com.alibaba.graphscope.fragment.adaptor.{
  ArrowProjectedAdaptor,
  ArrowProjectedStringEDAdaptor,
  ArrowProjectedStringVDAdaptor,
  ArrowProjectedStringVEDAdaptor
}
import com.alibaba.graphscope.fragment.getter.{
  ArrowFragmentGroupGetter,
  ArrowProjectedFragmentGetter,
  ArrowProjectedStringEDFragmentGetter,
  ArrowProjectedStringVDFragmentGetter,
  ArrowProjectedStringVEDFragmentGetter
}
import com.alibaba.graphscope.fragment.mapper.{
  ArrowProjectedFragmentMapper,
  ArrowProjectedStringEDFragmentMapper,
  ArrowProjectedStringVDFragmentMapper,
  ArrowProjectedStringVEDFragmentMapper
}
import com.alibaba.graphscope.fragment.{
  ArrowProjectedFragment,
  ArrowProjectedStringEDFragment,
  ArrowProjectedStringVDFragment,
  ArrowProjectedStringVEDFragment,
  IFragment
}
import com.alibaba.graphscope.graphx._
import com.alibaba.graphscope.stdcxx.{StdMap, StdVector}
import com.alibaba.graphscope.utils.CppClassName
import org.apache.spark.internal.Logging

import java.util.HashMap
import scala.reflect.ClassTag

object ScalaFFIFactory extends Logging {
  val clientFactory: VineyardClient.Factory = FFITypeFactory
    .getFactory(classOf[VineyardClient], "vineyard::Client")
    .asInstanceOf[VineyardClient.Factory]
  private val arrowArrayBuilderMap =
    new HashMap[String, ArrowArrayBuilder.Factory[_]]

  def newArrowArrayBuilder[T: ClassTag]: ArrowArrayBuilder[T] = {
    val clz = GrapeUtils.getRuntimeClass[T]
    synchronized {
      if (clz.equals(classOf[java.lang.Long]) || clz.equals(classOf[Long])) {
        getArrowArrayBuilderFactory("gs::ArrowArrayBuilder<int64_t>")
          .create()
          .asInstanceOf[ArrowArrayBuilder[T]]
      } else if (clz.equals(classOf[java.lang.Double]) || clz.equals(classOf[Double])) {
        getArrowArrayBuilderFactory("gs::ArrowArrayBuilder<double>")
          .create()
          .asInstanceOf[ArrowArrayBuilder[T]]
      } else if (clz.equals(classOf[Integer]) || clz.equals(classOf[Int])) {
        getArrowArrayBuilderFactory("gs::ArrowArrayBuilder<int32_t>")
          .create()
          .asInstanceOf[ArrowArrayBuilder[T]]
      } else throw new IllegalStateException("Not recognized " + clz.getName)
    }
  }
  def newArrowStringArrayBuilder: ArrowStringArrayBuilder = {
    val factory = FFITypeFactory
      .getFactory(classOf[ArrowStringArrayBuilder], "gs::ArrowArrayBuilder<std::string>")
      .asInstanceOf[ArrowStringArrayBuilder.Factory]
    factory.create()
  }

  def getArrowArrayBuilderFactory(
      foreignTypeName: String
  ): ArrowArrayBuilder.Factory[_] = synchronized {
    if (!arrowArrayBuilderMap.containsKey(foreignTypeName)) {
      synchronized {
        if (!arrowArrayBuilderMap.containsKey(foreignTypeName)) {
          arrowArrayBuilderMap.put(
            foreignTypeName,
            FFITypeFactory.getFactory(
              classOf[ArrowArrayBuilder[_]],
              foreignTypeName
            )
          )
        }
      }
    }
    arrowArrayBuilderMap.get(foreignTypeName)
  }

  def newVector[T: ClassTag]: StdVector[T] = synchronized {
    if (GrapeUtils.getRuntimeClass[T].equals(classOf[Long]))
      newLongVector.asInstanceOf[StdVector[T]]
    else if (GrapeUtils.getRuntimeClass[T].equals(classOf[Int]))
      newIntVector.asInstanceOf[StdVector[T]]
    else if (GrapeUtils.getRuntimeClass[T].equals(classOf[Double]))
      newDoubleVector.asInstanceOf[StdVector[T]]
    else
      throw new IllegalArgumentException(
        s"unsupported ${GrapeUtils.getRuntimeClass[T].getName}"
      )
  }

  def newLongVector: StdVector[Long] = synchronized {
    val factory = FFITypeFactory
      .getFactory(classOf[StdVector[Long]], "std::vector<int64_t>")
      .asInstanceOf[StdVector.Factory[Long]]
    factory.create()
  }

  def newIntVector: StdVector[Int] = synchronized {
    val factory = FFITypeFactory
      .getFactory(classOf[StdVector[Int]], "std::vector<int32_t>")
      .asInstanceOf[StdVector.Factory[Int]]
    factory.create()
  }

  def newDoubleVector: StdVector[Double] = synchronized {
    val factory = FFITypeFactory
      .getFactory(classOf[StdVector[Double]], "std::vector<double>")
      .asInstanceOf[StdVector.Factory[Double]]
    factory.create()
  }

  def newUnsignedLongArrayBuilder(): ArrowArrayBuilder[Long] = synchronized {
    getArrowArrayBuilderFactory("gs::ArrowArrayBuilder<uint64_t>")
      .create()
      .asInstanceOf[ArrowArrayBuilder[Long]]
  }

  def newSignedLongArrayBuilder(): ArrowArrayBuilder[Long] = synchronized {
    getArrowArrayBuilderFactory("gs::ArrowArrayBuilder<int64_t>")
      .create()
      .asInstanceOf[ArrowArrayBuilder[Long]]
  }

  def newSignedIntArrayBuilder(): ArrowArrayBuilder[Int] = synchronized {
    getArrowArrayBuilderFactory("gs::ArrowArrayBuilder<int32_t>")
      .create()
      .asInstanceOf[ArrowArrayBuilder[Int]]
  }

  def newVineyardArrayBuilder[T: ClassTag](
      client: VineyardClient,
      size: Int
  ): VineyardArrayBuilder[T] = {
    val factory = FFITypeFactory
      .getFactory(
        classOf[VineyardArrayBuilder[T]],
        "vineyard::ArrayBuilder<" + GrapeUtils.classToStr[T](true) + ">"
      )
      .asInstanceOf[VineyardArrayBuilder.Factory[T]]
    factory.create(client, size)
  }

  def newVineyardClient(): VineyardClient = synchronized {
    synchronized {
      clientFactory.create()
    }
  }

  def newProjectedFragmentMapper[
      NEW_VD: ClassTag,
      NEW_ED: ClassTag
  ]: ArrowProjectedFragmentMapper[Long, Long, NEW_VD, NEW_ED] =
    synchronized {
      val factory = FFITypeFactory
        .getFactory(
          classOf[ArrowProjectedFragmentMapper[Long, Long, NEW_VD, NEW_ED]],
          CppClassName.CPP_ARROW_PROJECTED_FRAGMENT_MAPPER + "<int64_t,uint64_t," + GrapeUtils
            .classToStr[NEW_VD](true) + "," + GrapeUtils.classToStr[NEW_ED](true) + ">"
        )
        .asInstanceOf[ArrowProjectedFragmentMapper.Factory[Long, Long, NEW_VD, NEW_ED]]
      factory.create()
    }

  def newProjectedStringVDFragmentMapper[
      NEW_VD: ClassTag,
      NEW_ED: ClassTag
  ]: ArrowProjectedStringVDFragmentMapper[Long, Long, NEW_ED] =
    synchronized {
      val factory = FFITypeFactory
        .getFactory(
          classOf[ArrowProjectedStringVDFragmentMapper[Long, Long, NEW_ED]],
          CppClassName.CPP_ARROW_PROJECTED_STRING_VD_FRAGMENT_MAPPER + "<int64_t,uint64_t," + GrapeUtils
            .classToStr[NEW_ED](true) + ">"
        )
        .asInstanceOf[ArrowProjectedStringVDFragmentMapper.Factory[Long, Long, NEW_ED]]
      factory.create()
    }

  def newProjectedStringEDFragmentMapper[
      NEW_VD: ClassTag,
      NEW_ED: ClassTag
  ]: ArrowProjectedStringEDFragmentMapper[Long, Long, NEW_VD] =
    synchronized {
      val factory = FFITypeFactory
        .getFactory(
          classOf[ArrowProjectedStringEDFragmentMapper[Long, Long, NEW_ED]],
          CppClassName.CPP_ARROW_PROJECTED_STRING_ED_FRAGMENT_MAPPER + "<int64_t,uint64_t," + GrapeUtils
            .classToStr[NEW_VD](true) + ">"
        )
        .asInstanceOf[ArrowProjectedStringEDFragmentMapper.Factory[Long, Long, NEW_VD]]
      factory.create()
    }
  def newProjectedStringVEDFragmentMapper[
      NEW_VD: ClassTag,
      NEW_ED: ClassTag
  ]: ArrowProjectedStringVEDFragmentMapper[Long, Long] =
    synchronized {
      val factory = FFITypeFactory
        .getFactory(
          classOf[ArrowProjectedStringVEDFragmentMapper[Long, Long]],
          CppClassName.CPP_ARROW_PROJECTED_STRING_VED_FRAGMENT_MAPPER + "<int64_t,uint64_t>"
        )
        .asInstanceOf[ArrowProjectedStringVEDFragmentMapper.Factory[Long, Long]]
      factory.create()
    }

  def newArrowProjectedFragmentGetter[VD: ClassTag, ED: ClassTag]
      : ArrowProjectedFragmentGetter[Long, Long, VD, ED] = {
    val getterStr = CppClassName.CPP_ARROW_PROJECTED_FRAGMENT_GETTER + "<int64_t,uint64_t," +
      GrapeUtils.classToStr[VD](true) + "," + GrapeUtils.classToStr[ED](true) + ">"
    val factory = FFITypeFactory
      .getFactory(
        classOf[ArrowProjectedFragmentGetter[Long, Long, VD, ED]],
        getterStr
      )
      .asInstanceOf[ArrowProjectedFragmentGetter.Factory[Long, Long, VD, ED]]
    factory.create()
  }

  def newArrowProjectedStringVDFragmentGetter[VD: ClassTag, ED: ClassTag]
      : ArrowProjectedStringVDFragmentGetter[Long, Long, ED] = {
    val getterStr = CppClassName.CPP_ARROW_PROJECTED_STRING_VD_FRAGMENT_GETTER + "<int64_t,uint64_t," +
      GrapeUtils.classToStr[ED](true) + ">"
    val factory = FFITypeFactory
      .getFactory(
        classOf[ArrowProjectedStringVDFragmentGetter[Long, Long, ED]],
        getterStr
      )
      .asInstanceOf[ArrowProjectedStringVDFragmentGetter.Factory[Long, Long, ED]]
    factory.create()
  }

  def newArrowProjectedStringEDFragmentGetter[VD: ClassTag, ED: ClassTag]
      : ArrowProjectedStringEDFragmentGetter[Long, Long, VD] = {
    val getterStr = CppClassName.CPP_ARROW_PROJECTED_STRING_ED_FRAGMENT_GETTER + "<int64_t,uint64_t," +
      GrapeUtils.classToStr[VD](true) + ">"
    val factory = FFITypeFactory
      .getFactory(
        classOf[ArrowProjectedStringEDFragmentGetter[Long, Long, VD]],
        getterStr
      )
      .asInstanceOf[ArrowProjectedStringEDFragmentGetter.Factory[Long, Long, VD]]
    factory.create()
  }

  def newArrowProjectedStringVEDFragmentGetter[VD: ClassTag, ED: ClassTag]
      : ArrowProjectedStringVEDFragmentGetter[Long, Long] = {
    val getterStr = CppClassName.CPP_ARROW_PROJECTED_STRING_VED_FRAGMENT_GETTER + "<int64_t,uint64_t>"
    val factory = FFITypeFactory
      .getFactory(
        classOf[ArrowProjectedStringVEDFragmentGetter[Long, Long]],
        getterStr
      )
      .asInstanceOf[ArrowProjectedStringVEDFragmentGetter.Factory[Long, Long]]
    factory.create()
  }

  def getFragment[VD: ClassTag, ED: ClassTag](
      client: VineyardClient,
      objectID: Long,
      fragStr: String
  ): IFragment[Long, Long, VD, ED] = synchronized {
    if (fragStr.startsWith("gs::ArrowFragment")) {
      throw new IllegalStateException("Not implemented now")
    } else if (fragStr.startsWith("gs::ArrowProjectedFragment")) {
      log.info(s"Getting fragment for ${fragStr}, ${objectID}")
      if (GrapeUtils.isPrimitive[VD] && GrapeUtils.isPrimitive[ED]) {
        val getter = newArrowProjectedFragmentGetter[VD, ED]
        val res    = getter.get(client, objectID)
        new ArrowProjectedAdaptor[Long, Long, VD, ED](res.get())
      } else if (GrapeUtils.isPrimitive[VD] && !GrapeUtils.isPrimitive[ED]) {
        val getter = newArrowProjectedStringEDFragmentGetter[VD, ED]
        val res    = getter.get(client, objectID)
        new ArrowProjectedStringEDAdaptor[Long, Long, VD](
          res.get()
        ).asInstanceOf[IFragment[Long, Long, VD, ED]]
      } else if (!GrapeUtils.isPrimitive[VD] && GrapeUtils.isPrimitive[ED]) {
        val getter = newArrowProjectedStringVDFragmentGetter[VD, ED]
        val res    = getter.get(client, objectID)
        new ArrowProjectedStringVDAdaptor[Long, Long, ED](res.get()).asInstanceOf[IFragment[Long, Long, VD, ED]]
      } else if (!GrapeUtils.isPrimitive[VD] && !GrapeUtils.isPrimitive[ED]) {
        val getter = newArrowProjectedStringVEDFragmentGetter[VD, ED]
        val res    = getter.get(client, objectID)
        new ArrowProjectedStringVEDAdaptor[Long, Long](res.get()).asInstanceOf[IFragment[Long, Long, VD, ED]]
      } else {
        throw new IllegalStateException("not possible")
      }
    } else {
      throw new IllegalStateException(s"Not recognized frag str ${fragStr}")
    }
  }

  /** transform the frag name to frag getter name. */
  def fragName2FragGetterName(str: String): String = synchronized {
    if (str.contains("ArrowProjectedFragment")) {
      str.replace("ArrowProjectedFragment", "ArrowProjectedFragmentGetter")
    } else if (str.contains("ArrowFragment")) {
      str.replace("ArrowFragment", "ArrowFragmentGetter")
    } else {
      throw new IllegalStateException(s"Not recognized ${str}")
    }
  }

  def newFragmentGroupGetter(): ArrowFragmentGroupGetter = {
    val factory = FFITypeFactory
      .getFactory(
        classOf[ArrowFragmentGroupGetter],
        "gs::ArrowFragmentGroupGetter"
      )
      .asInstanceOf[ArrowFragmentGroupGetter.Factory]
    factory.create()
  }

  def newStdMap[K: ClassTag, V: ClassTag](
      signed: Boolean = true
  ): StdMap[K, V] = {
    val factory = FFITypeFactory
      .getFactory(
        classOf[StdMap[K, V]],
        "std::map<" + GrapeUtils
          .classToStr(GrapeUtils.getRuntimeClass[K], signed) + "," +
          GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[V], signed) + ">"
      )
      .asInstanceOf[StdMap.Factory[K, V]]
    factory.create()
  }

  def newRawDataBuilder[
      OID_T: ClassTag,
      VID_T: ClassTag,
      VD_T: ClassTag,
      ED_T: ClassTag
  ](
      client: VineyardClient,
      oids: StdVector[OID_T],
      vdatas: StdVector[VD_T],
      srcOids: StdVector[OID_T],
      dstOids: StdVector[OID_T],
      edatas: StdVector[ED_T]
  ): GraphXRawDataBuilder[OID_T, VID_T, VD_T, ED_T] = {
    val factory = FFITypeFactory
      .getFactory(
        classOf[GraphXRawDataBuilder[OID_T, VID_T, VD_T, ED_T]],
        CppClassName.GS_GRAPHX_RAW_DATA_BUILDER + "<" +
          GrapeUtils.classToStr[OID_T](true) + "," +
          GrapeUtils.classToStr[VID_T](false) + "," +
          GrapeUtils.classToStr[VD_T](true) + "," +
          GrapeUtils.classToStr[ED_T](true) + ">"
      )
      .asInstanceOf[GraphXRawDataBuilder.Factory[OID_T, VID_T, VD_T, ED_T]]
    log.info(s"Creating GraphX raw data builder with vd ${GrapeUtils.classToStr[VD_T](true)}, ED ${GrapeUtils
      .classToStr[ED_T](true)}")
    factory.create(client, oids, vdatas, srcOids, dstOids, edatas)
  }

}
