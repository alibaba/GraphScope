package RDDReaderTransfer

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.jdk.CollectionConverters._
import Array._

object TestScala {
  //这个函数返回的是每个executor对应的worker ip，除了driver
  /*def getExecutorList(sc : SparkContext) : Array[String] = {
    val allExecutors = sc.getExecutorMemoryStatus.map(_._1).toList
    val allHosts = allExecutors.map(_.split(":")(0)).distinct
    return allHosts
    //val driverHost: String = sc.getConf.get("spark.driver.host")
    //return allExecutors.filter(! _.split(":")(0).equals(driverHost)).toList
  }*/

  val node_executors: Map[String, Int] = Map()

  def getExecutorId(hostName: String): Int = {
    this.synchronized {
      if (node_executors.contains(hostName)) {
        node_executors(hostName) = node_executors(hostName) + 1
      } else {
        node_executors += (hostName -> 0)
      }
      return node_executors(hostName)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDTest")
    val sc = new SparkContext(conf)

    val RDDSize = 100
    val ArrSize = 100

    val essential_names = Array[String]("int", "double", "float", "long", "bool", "string")
    val tuple_name = "tuple"

    val RDDData = new Array[(Int,Array[Int])](RDDSize)

    for(i <- 0 until RDDSize) {
      val list = range(i, ArrSize, 1)
      val item = (i, list)
      RDDData(i) = item
    }

    val rdd = sc.parallelize(RDDData)
    val data_tmp = rdd.first()
    var datatype = data_tmp.getClass.getSimpleName.toLowerCase()

    var find_type = false
    for(essen_name <- essential_names if !find_type) {
      if(datatype.startsWith(essen_name)){
        if(datatype.endsWith("[]")) {
          datatype = "Array," + essen_name.toLowerCase()
        }else{
          datatype = essen_name
        }
        find_type = true
      }
    }

    if(!find_type) {
      if(datatype.startsWith(tuple_name)) {
        datatype = "tuple"
        for(iter <- data_tmp.productIterator) {
          val part_type = iter.getClass.getSimpleName.toLowerCase()
          if(part_type.endsWith("[]")) {
            datatype = datatype + ":Array," + part_type.replaceAll("\\[\\]", "")
          }else {
            datatype = datatype + ":" + iter.getClass.getSimpleName.toLowerCase()
          }
        }
      }
    }

    println(datatype)

    rdd.foreachPartition(iter => {
      val cur_host = RDDReadServer.getLocalHostLANAddress()
      val executor_id = getExecutorId(cur_host)
      val server = new RDDReadServer(executor_id, TaskContext.get.partitionId, iter.asJava, datatype)
      server.start()
      server.blockUntilShutdown()
    })
  }
}
