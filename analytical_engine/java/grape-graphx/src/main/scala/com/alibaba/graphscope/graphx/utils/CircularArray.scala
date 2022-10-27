package com.alibaba.graphscope.graphx.utils

import scala.reflect.ClassTag

class CircularArray[T: ClassTag] extends Serializable {
  val array = new PrimitiveVector[T]()
  var cur   = 0;
  def addValue(value: T): Unit = {
    array.+=(value)
  }

  def getNext(): T = {
    cur = (cur + 1) % array.size
    array(cur)
  }
}
