import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.util.Date
import java.text.SimpleDateFormat
import NewGraphLoader._

object PageRankExample {
  def main(args: Array[String]): Unit = {

    val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val date = new Date()
    println(s"Start Time:")
    println(dateFormat.format(date))

    val spark = SparkSession.builder
      .appName("PageRankExample")
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

    // 运行PageRank
    val ranks = graph.pageRank(10).vertices

    val date3 = new Date()
    println(s"Running Time:")
    println(dateFormat.format(date3))

    // 输出结果
    // ranks.collect().foreach { case (id, rank) => println(s"$id has rank $rank") }
    println(s"Finish PageRank")

    spark.stop()
  }
}
