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

import java.io._

/** Serialize a function obj to a path;
  * deserialize a function obj from path
  */
object SerializationUtils extends Logging {
  def write[A](path: String, objs: Any*): Unit = {
    log.info("Write obj " + objs.mkString("Array(", ", ", ")") + " to :" + path)
    val bo = new FileOutputStream(new File(path))
    val outputStream = new ObjectOutputStream(bo)
    outputStream.writeInt(objs.length)
    var i = 0
    while (i < objs.length) {
      if (objs(i).equals(classOf[Long])) {
        outputStream.writeObject(classOf[java.lang.Long])
      } else if (objs(i).equals(classOf[Int])) {
        outputStream.writeObject(classOf[java.lang.Integer])
      } else if (objs(i).equals(classOf[Double])) {
        outputStream.writeObject(classOf[java.lang.Double])
      } else {
        outputStream.writeObject(objs(i))
      }
      i += 1
    }
    outputStream.flush()
    outputStream.close()
  }

  @throws[ClassNotFoundException]
  def read(classLoader: ClassLoader, filepath: String): Array[Any] = {
    log.info(
      "Reading from file path: " + filepath + ", with class loader: " + classLoader
    )
    val objectInputStream = new ObjectInputStream(
      new FileInputStream(new File(filepath))
    ) {
      @throws[IOException]
      @throws[ClassNotFoundException]
      protected override def resolveClass(desc: ObjectStreamClass): Class[_] = {
        if (desc.getName.equals("long")) {
          classOf[java.lang.Long]
        } else if (desc.getName.equals("double")) {
          classOf[java.lang.Double]
        } else if (desc.getName.equals("int")) {
          classOf[java.lang.Integer]
        } else {
          Class.forName(desc.getName, false, classLoader)
        }
      }
    }
    val len = objectInputStream.readInt()
    val res = new Array[Any](len)
    for (i <- 0 until len) {
      res(i) = objectInputStream.readObject()
    }
    res
  }
}
