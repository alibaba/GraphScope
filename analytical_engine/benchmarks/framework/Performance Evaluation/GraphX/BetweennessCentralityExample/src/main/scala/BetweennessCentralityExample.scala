import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.util.Date
import java.text.SimpleDateFormat
import NewGraphLoader._
import scala.collection.mutable.ArrayBuffer

object BetweennessCentralityExample {

  def BFS(graph: Graph[Int, Int], root: Int): Graph[(Int, Int, Int, Boolean), Int] = {
    val initialGraph = graph.mapVertices((id, _) => 
      if (id == root) (0, 1, 0, true)
      else (Int.MaxValue, 0, 0, false)
    )

    val bfsGraph = initialGraph.pregel((Int.MaxValue, 0, 0), Int.MaxValue, EdgeDirection.Out)(
      (id, oldAttr, newAttr) => {
        if (newAttr._1 == Int.MaxValue) // only happened in the first iteration
          oldAttr
        
        else if (newAttr._1 < oldAttr._1 && oldAttr._1 == Int.MaxValue)  // first visited
          (newAttr._1, newAttr._2, newAttr._3, true)

        else if (newAttr._1 - 1 == oldAttr._1) // backtrace edge, only add succ
          (oldAttr._1, oldAttr._2, oldAttr._3 + newAttr._3, false)

        else 
          throw new RuntimeException("never happens" + oldAttr._1 + " " + newAttr._1)
      },

      triplet => {
        if (triplet.srcAttr._4 == false || triplet.srcAttr._1 == Int.MaxValue) { // inactive
          Iterator.empty
        } else if (triplet.dstAttr._1 == Int.MaxValue) {  // next layer
          Iterator((triplet.dstId, (triplet.srcAttr._1 + 1, triplet.srcAttr._2, 0)))
        } else if (triplet.srcAttr._1 == triplet.dstAttr._1) {  // same layer
          Iterator.empty
        } else if (triplet.srcAttr._1 - 1 == triplet.dstAttr._1) {  // backtrace layer
          Iterator((triplet.dstId, (triplet.srcAttr._1, 0, 1)))
        } else if (triplet.srcAttr._1 + 1 == triplet.dstAttr._1) {  // already computed
          Iterator.empty
        } else {
          throw new RuntimeException("never happens" + triplet.srcAttr._1 + " " + triplet.dstAttr._1)
        }
      },

      (a, b) => 
        if (a._1 < b._1) a 
        else if (a._1 == b._1) (a._1, a._2 + b._2, a._3 + b._3) 
        else b
    )
    bfsGraph
  }

  // attr = (distance, number, succ, centrality, status)
  // msg = (succ, centrality)
  def Betweenness(graph: Graph[(Int, Int, Int, Boolean), Int]): Graph[(Int, Int, Int, Double, Boolean), Int] = {
    val initialGraph = graph.mapVertices((id, attr) => 
      (attr._1, attr._2, attr._3, 0.0, (attr._3 == 0))
    )

    val bcGraph = initialGraph.pregel((Int.MaxValue, 0.0), Int.MaxValue, EdgeDirection.Out)(
      (id, oldAttr, newAttr) => {
        if (newAttr._1 == Int.MaxValue)   // only happen in the first iteration
          oldAttr
        else if (oldAttr._3 != 0 && oldAttr._3 == newAttr._1)    // just deleted and active
          (oldAttr._1, oldAttr._2, oldAttr._3 - newAttr._1, oldAttr._4 + newAttr._2, true)
        else if (oldAttr._3 != 0 && oldAttr._3 != newAttr._1)
          (oldAttr._1, oldAttr._2, oldAttr._3 - newAttr._1, oldAttr._4 + newAttr._2, false)
        else
          throw new RuntimeException("never happens" + oldAttr._1 + " " + newAttr._1)
      },

      triplet => {
        if (triplet.srcAttr._5 == false) { // inactive
          Iterator.empty
        } else if (triplet.srcAttr._1 - 1 == triplet.dstAttr._1) {  // backtrace layer
          Iterator((triplet.dstId, (1, 1.0 / triplet.srcAttr._2 * (1 + triplet.srcAttr._4))))
        } else if (triplet.srcAttr._1 == triplet.dstAttr._1) {  // same layer
          Iterator.empty
        } else if (triplet.srcAttr._1 + 1 == triplet.dstAttr._1) {  // already computed
          Iterator.empty
        } else {
          throw new RuntimeException("never happens" + triplet.srcAttr._1 + " " + triplet.dstAttr._1)
        }
      },

      (a, b) => (a._1 + b._1, a._2 + b._2)
    )
    bcGraph
  }

  def main(args: Array[String]): Unit = {

    val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val date = new Date()
    println("Start Time:")
    println(dateFormat.format(date))

    val spark = SparkSession.builder
      .appName("BetweennessCentralityExample")
      .getOrCreate()
    implicit val sc = spark.sparkContext

    val date1 = new Date()
    println("Preparing Time:")
    println(dateFormat.format(date1))

    // 输入图
    val graph = NewGraphLoader.edgeListFile(sc, "/graphx_data/input.txt", numEdgePartitions=args(0).toInt)

    val date2 = new Date()
    println("Loading Time:")
    println(dateFormat.format(date2))

    // 运行Betweenness
    val bfsGraph = BFS(graph, root=0)
    val bcGraph = Betweenness(bfsGraph)

    val date3 = new Date()
    println("Running Time:")
    println(dateFormat.format(date3))

    // 输出结果
    println("Finish Betweenness Centrality")
    // bcGraph.vertices.collect.foreach { case (id, (dist, count, succ, centrality, status)) =>
    //   println("Vertex $id: distance = $dist, number of shortest paths = $count, centrality value = $centrality")
    // }

    val date4 = new Date()
    println("Finish Time:")
    println(dateFormat.format(date4))

    spark.stop()
  }
}
