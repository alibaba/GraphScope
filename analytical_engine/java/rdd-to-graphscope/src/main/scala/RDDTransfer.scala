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

    val root_dir = "/Users/anwh/git/RDDReaderTransfer/"
    val nodes = sc.textFile(root_dir + "new_nodes.txt").map(line => line.split(","))
      .map(parts =>(parts.head.toLong,parts.drop(1)))

    val node_type = "tuple:long:Array,string"
    println(node_type)

    nodes.foreachPartition(iter => {
      val cur_host = RDDReadServer.getLocalHostLANAddress()
      val executor_id = getExecutorId(cur_host)
      val server = new RDDReadServer(executor_id, TaskContext.get.partitionId, iter.asJava, node_type, nodes.getNumPartitions)
      server.start()
      server.blockUntilShutdown()
    })
    println("node transfer over")

    val edges = sc.textFile(root_dir + "new_edges.txt").map(line => line.split(","))
      .map(parts => (parts(0).toLong, parts(1).toLong, parts.drop(2)))

    val edge_type = "tuple:long:long:Array,string"
    println(edge_type)

    edges.foreachPartition(iter => {
      val cur_host = RDDReadServer.getLocalHostLANAddress()
      val executor_id = getExecutorId(cur_host)
      val server = new RDDReadServer(executor_id, TaskContext.get.partitionId, iter.asJava, edge_type, edges.getNumPartitions)
      server.start()
      server.blockUntilShutdown()
    })
    println("edge transfer over")
    println("graph transfer all over")
  }
}
