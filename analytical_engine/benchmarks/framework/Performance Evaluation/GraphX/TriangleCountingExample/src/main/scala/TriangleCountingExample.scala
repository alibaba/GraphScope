import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
import java.util.Date
import java.text.SimpleDateFormat
import NewGraphLoader._

object TriangleCountingExample {
  def main(args: Array[String]): Unit = {

    val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val date = new Date()
    println(s"Start Time:")
    println(dateFormat.format(date))

    val spark = SparkSession.builder
      .appName("TriangleCountingExample")
      .getOrCreate()
    val sc = spark.sparkContext

    val date1 = new Date()
    println(s"Preparing Time:")
    println(dateFormat.format(date1))

    // 导入图
    val graph = NewGraphLoader.edgeListFile(sc, "/graphx_data/input.txt", numEdgePartitions=args(0).toInt, canonicalOrientation=true).partitionBy(PartitionStrategy.RandomVertexCut)

    val date2 = new Date()
    println(s"Loading Time:")
    println(dateFormat.format(date2))

    // 运行Triangle Counting
    // val triangleCounts = graph.triangleCount().vertices
    // 保证是单向边
    val triangleCounts = TriangleCount.runPreCanonicalized(graph).vertices

    val date3 = new Date()
    println(s"Running Time:")
    println(dateFormat.format(date3))

    // 输出结果
    //triangleCounts.collect().foreach { case (vertexId, count) =>
    //  println(s"Vertex $vertexId has $count triangle(s)")
    //}
    //val maxTriangleCountVertex = triangleCounts.reduce((a, b) => if (a._2 > b._2) a else b)
    //println(maxTriangleCountVertex)

    println(triangleCounts.collect()(0))
    println(s"Finish TriangleCounting")

    val date4 = new Date()
    println(s"Finish Time:")
    println(dateFormat.format(date4))

    spark.stop()
  }
}
