import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.util.Date
import java.text.SimpleDateFormat
import NewGraphLoader._

object KCliqueExample {

  def KCliqueCounting(subgraph: Map[VertexId, Array[VertexId]], cand: Set[VertexId], lev: Long): Long = {
    if (lev == 5 - 1) {
      return cand.size
    }
    var t: Long = 0L
    for (u <- cand) {
      var next_cand: Set[VertexId] = Set()
      for (v <- subgraph(u)) {
        if (cand.contains(v)) {
          next_cand += v
        }
      }
      if (next_cand.size >= 5 - lev - 1) {
        t += KCliqueCounting(subgraph, next_cand, lev + 1)
      }
    }
    return t
  }

  def main(args: Array[String]): Unit = {

    val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val date = new Date()
    println(s"Start Time:")
    println(dateFormat.format(date))

    val spark = SparkSession.builder
      .appName("KCliqueExample")
      .getOrCreate()
    val sc = spark.sparkContext

    val date1 = new Date()
    println(s"Preparing Time:")
    println(dateFormat.format(date1))

    // 导入图
    val graph = NewGraphLoader.edgeListFile(sc, "/graphx_data/input.txt", numEdgePartitions=args(0).toInt)
    val date2 = new Date()
    println(s"Loading Time:")
    println(dateFormat.format(date2))

    // 获取邻居
    val neighbors: VertexRDD[Array[VertexId]] = graph.collectNeighbors(EdgeDirection.In).mapValues(attr => attr.map(_._1))
    val nb_graph = Graph(neighbors, graph.edges).mapVertices((id, list) => list.filter( _ > id ))
    // nb_graph.vertices.collect().foreach {
    //   case (id, neighborList) => {
    //     println(s"Node $id has neighbors: ${neighborList.mkString(", ")}")
    //   }
    // }

    // 获取二阶邻居
    val second_nb: VertexRDD[Array[(VertexId, Array[VertexId])]] = nb_graph.collectNeighbors(EdgeDirection.In)
    val second_graph = Graph(second_nb, graph.edges).mapVertices((id, list) => list.filter(_._1 > id))
    // second_graph.vertices.collect().foreach {
    //   case (id, neighborList) => {
    //     println(s"Node $id has neighbors:")
    //     for ((v, list) <- neighborList) {
    //       println(s"Vertex $v : ${list.mkString(", ")}")
    //     }
    //   }
    // }

    // 构造子图并计算
    val answer_graph = second_graph.mapVertices {
      (id, neighborList) => {
        var subgraph: Map[VertexId, Array[VertexId]] = neighborList.toMap
        var cand: Set[VertexId] = neighborList.map( attr => attr._1 ).toSet
        val count: Long = KCliqueCounting(subgraph, cand, 1L)
        count
        // println(s"Vertex $id has $count kcliques")
      }
    }
    val date3 = new Date()
    println(s"Running Time:")
    println(dateFormat.format(date3))

    val answer: Long = answer_graph.vertices.map(_._2).reduce( _ + _ )
    println(s"*************************")
    println(s"*************************")
    println(s"Total K-Clique is $answer")
    println(s"*************************")
    println(s"*************************")

    val date4 = new Date()
    println(s"Finish Time:")
    println(dateFormat.format(date4))

    spark.stop()
  }
}
