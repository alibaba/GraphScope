package com.alibaba.graphscope.graphx.shuffle

import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.OpenHashSet

import scala.reflect.ClassTag

abstract class DataShuffle[VD, ED](
    val dstPid: Int,
    val srcOids: Array[Long],
    val dstOids: Array[Long]
) extends Serializable {
  if (srcOids != null) {
    require(dstOids != null, "should be both non-null")
    require(srcOids.length == dstOids.length, "Edge src and dst array neq")
  }

  def numVertices: Long

  def numEdges: Long

  def getOidIterator: Iterator[Long]

  def getVdataIterator: Iterator[VD]

  def getEdataIterator: Iterator[ED]
}

/** The shuffled graph data are with default values, no custom value
  */
class DefaultDataShuffle[VD: ClassTag, ED: ClassTag](
    dstPid: Int,
    val oids: OpenHashSet[Long],
    srcOids: Array[Long],
    dstOids: Array[Long],
    defaultVD: VD,
    defaultED: ED
) extends DataShuffle[VD, ED](dstPid, srcOids, dstOids)
    with Logging {

  override def getOidIterator: Iterator[Long] = oids.iterator

  override def getVdataIterator: Iterator[VD] = Iterator(defaultVD)

  override def getEdataIterator: Iterator[ED] = Iterator(defaultED)

  override def numVertices: Long = oids.size

  override def numEdges: Long = srcOids.length
}

class CustomDataShuffle[VD: ClassTag, ED: ClassTag](
    dstPid: Int,
    val oids: Array[Long],
    val vdatas: Array[VD],
    srcOids: Array[Long],
    dstOids: Array[Long],
    val edatas: Array[ED]
) extends DataShuffle[VD, ED](dstPid, srcOids, dstOids)
    with Logging {

  def this(dstPid: Int, oids: Array[Long], vdatas: Array[VD]) {
    this(dstPid, oids, vdatas, null, null, null)
  }

  def this(dstPid: Int, srcOids: Array[Long], dstOids: Array[Long], edatas: Array[ED]) {
    this(dstPid, null, null, srcOids, dstOids, edatas)
  }

  override def getOidIterator: Iterator[Long] = {
    if (oids == null) {
      Iterator.empty
    } else oids.iterator
  }

  override def getVdataIterator: Iterator[VD] = {
    if (vdatas == null) {
      Iterator.empty
    }
    vdatas.iterator
  }

  override def getEdataIterator: Iterator[ED] = {
    if (edatas == null) Iterator.empty
    else edatas.iterator
  }

  override def numVertices: Long = {
    if (oids == null) {
      0
    } else oids.length
  }

  override def numEdges: Long = {
    if (srcOids == null) {
      0
    } else srcOids.length
  }
}
