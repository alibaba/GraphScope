package com.alibaba.graphscope.graphx.shuffle

import org.apache.spark.internal.Logging

import java.util.concurrent.ArrayBlockingQueue
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class DataShuffleHolder[VD: ClassTag, ED: ClassTag] extends Serializable {
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
  val queue = new ArrayBlockingQueue[DataShuffleHolder[_, _]](1024000)
//  var array        = null.asInstanceOf[Array[DataShuffleHolder[_, _]]]

  def push(in: DataShuffleHolder[_, _]): Unit = {
    require(queue.offer(in))
  }

  /** This method will return all data shuffle holders in one array, and then clean store. If no data is available,
    * just return null.
    * @return
    */
  def popAll: Array[DataShuffleHolder[_, _]] = {
//    if (array == null) {
    if (!queue.isEmpty()) {
      log.info(s"convert to array size ${queue.size()}")
      val array = new Array[DataShuffleHolder[_, _]](queue.size())
      queue.toArray[DataShuffleHolder[_, _]](array)
      array
    } else {
      log.warn("no data shuffle holder in this executor, returning null")
      null
    }
  }
}
