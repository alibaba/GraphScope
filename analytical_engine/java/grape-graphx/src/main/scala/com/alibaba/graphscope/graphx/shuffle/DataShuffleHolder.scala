package com.alibaba.graphscope.graphx.shuffle

import org.apache.spark.internal.Logging

import java.util.concurrent.ArrayBlockingQueue
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class DataShuffleHolder[VD: ClassTag, ED: ClassTag] extends Logging {
  val fromPid2Shuffle = new ArrayBuffer[DataShuffle[VD, ED]]

  def add(edgeShuffle: DataShuffle[VD, ED]): Unit = {
    fromPid2Shuffle.+=(edgeShuffle)
  }

  override def toString: String = {
    var res = "DataShuffleHolder @Partition: "
    for (shuffle <- fromPid2Shuffle) {
      res += s"(receive vertices ${shuffle.numVertices}, edges ${shuffle.numEdges});"
    }
    res
  }
}

object DataShuffleHolder extends Logging {
  val queue        = new ArrayBlockingQueue[DataShuffleHolder[_, _]](1024000)
  val distinctPids = null.asInstanceOf[Array[Int]]
  var array        = null.asInstanceOf[Array[DataShuffleHolder[_, _]]]

  def push(in: DataShuffleHolder[_, _]): Unit = {
    require(queue.offer(in))
  }

  def getArray: Array[DataShuffleHolder[_, _]] = {
    if (array == null) {
      log.info(s"convert to array size ${queue.size()}")
      array = new Array[DataShuffleHolder[_, _]](queue.size())
      queue.toArray[DataShuffleHolder[_, _]](array)
    }
    array
  }
}
