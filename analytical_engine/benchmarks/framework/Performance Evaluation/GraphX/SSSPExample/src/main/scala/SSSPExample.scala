import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.util.Date
import java.text.SimpleDateFormat
import NewGraphLoader._

object SSSPExample {
  def main(args: Array[String]): Unit = {

    val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val date = new Date()
    println(s"Start Time:")
    println(dateFormat.format(date))

    val spark = SparkSession.builder
      .appName("SSSPExample")
      .getOrCreate()
    val sc = spark.sparkContext

    val date1 = new Date()
    println(s"Preparing Time:")
    println(dateFormat.format(date1))

    // Load the edges as a graph
    val graph = NewGraphLoader.edgeListFile(sc, "/graphx_data/input.txt", numEdgePartitions=args(0).toInt)

    val date2 = new Date()
    println(s"Loading Time:")
    println(dateFormat.format(date2))

    // 设置起点顶点 ID
    val sourceVertexId: VertexId = 0L
    // 使用 ShortestPaths 计算从起始顶点到所有其他顶点的最短路径
    val shortestPaths = ShortestPaths.run(graph, Seq(sourceId))

    // 初始化图中顶点的属性，起点顶点为 0，其他顶点为无穷大
    //val initialGraph = graph.mapVertices { (vid, _) =>
    //  if (vid == sourceVertexId) 0.0 else Double.PositiveInfinity
    //}
    // 执行最短路径算法，使用 pregel 方法
    //val shortestPaths = initialGraph.pregel(Double.PositiveInfinity)(
    //  (id, dist, newDist) => math.min(dist, newDist), // 更新消息
    //  triplet => {  // 发送消息
    //    if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
    //      Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
    //    } else {
    //      Iterator.empty
    //    }
    //  },
    //  (a, b) => math.min(a, b) // 合并消息
    //)


    // 提取结果并打印最短路径
    //val shortestPathsWithSource = shortestPaths.vertices.mapValues((vid, dist) =>
    //  s"Vertex $vid shortest distance: ${if (dist == Double.PositiveInfinity) "No path" else dist}"
    //)

    val date3 = new Date()
    println(s"Running Time:")
    println(dateFormat.format(date3))

    // 输出结果
    // shortestPathsWithSource.collect().foreach(println)
    println(s"Finish SSSP")

    spark.stop()
  }
}
