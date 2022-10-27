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

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

@RunWith(classOf[JUnitRunner])
class CaseClassTest extends FunSuite{
  def write[A](obj: A): Array[Byte] = {
    val bo = new ByteArrayOutputStream()
    new ObjectOutputStream(bo).writeObject(obj)
    bo.toByteArray
  }

  def read(bytes:Array[Byte]): Any = {
    new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject()
  }

  test("test1"){
    println(read(write( {a:Int => a+1} )).asInstanceOf[ Function[Int,Int] ](5)) // == 6
  }

}
