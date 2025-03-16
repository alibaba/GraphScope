import org.apache.spark._
import org.apache.spark.graphx._
import scala.reflect.ClassTag
import java.util.Date
import java.text.SimpleDateFormat

/**
  * Calculate betweenness centrality
  */

object Betweenness {
  type OriginV = VertexId
  type Distance = Int

  // number of shortest paths
  type NumSP = Long

  type Betweenness = Double

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], landmarks: Seq[VertexId] = Seq.empty)
                                     (implicit sc: SparkContext): Graph[Double, ED] = {
    val g = TraversalStep.run(graph, landmarks)

    val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val date = new Date()
    println("Middle Time:")
    println(dateFormat.format(date))

    AccumulationStep.run(g)
  }

  object TraversalStep {

    // If a node is adjacent to aonother and its distance is one less than the other, it must be the previous node.
    // Therefore, we do not store the previous node ids.
    type NewInfo = Map[OriginV, (Distance, NumSP)]
    type OldInfo = NewInfo
    type Attr = (NewInfo, OldInfo)
    type Msg = NewInfo

    private[this] def mergeMsg(a: Msg, b: Msg): Msg = (a.keySet ++ b.keySet).map { k =>
      k -> ((a.get(k), b.get(k)) match {
        case (Some((d1, s1)), Some((d2, s2))) => (d1, s1 + s2)
        case (Some(x), None) => x
        case (None, Some(y)) => y
        case _ => throw new RuntimeException("never happens")
      })
    }.toMap

    // Assuming that all information in "msg" is new.
    private[this] def vertexProgram(id: VertexId, attr: Attr, msg: Msg): Attr = {
      if (msg.isEmpty) attr else if (attr._1.size > attr._2.size) (msg, attr._1 ++ attr._2) else (msg, attr._2 ++ attr._1)
    }

    // Send information that exists in "src" and not in "dst."
    private[this] def sendMessage(edge: EdgeTriplet[Attr, _]): Iterator[(VertexId, Msg)] = {
      edge.dstAttr // Note: [workaround] This is necessary to avoid NullPointerException in the "filterKeys" method.

      // Note: This will be the most time-consuming.
      val update = edge.srcAttr._1.withFilter { case (k, v) => !edge.dstAttr._2.contains(k) && !edge.dstAttr._1.contains(k) }

      // Sum up "distance" and the number shortest paths.
      val aggregated = update.map { case (s, (distance, numSP)) => s -> ((distance + 1, numSP)) }

      if (aggregated.isEmpty)
        Iterator.empty
      else
        Iterator((edge.dstId, aggregated))
    }

    val initialMsg: Msg = Map.empty

    /**
      * @param graph     graph
      * @param landmarks If empty, set all nodes as landmarks.
      */
    def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], landmarks: Seq[VertexId]): Graph[Attr, ED] = {
      // Set initial attributes.
      val g1: Graph[Attr, ED] = graph.mapVertices { (vid, attr) =>
        (
          if (landmarks.isEmpty || landmarks.contains(vid))
            Map(vid -> ((0, 1L)))
          else
            Map.empty[OriginV, (Distance, NumSP)],
          Map.empty[OriginV, (Distance, NumSP)]
        )
      }
      // Add the inverted edges.
      val g2: Graph[Attr, ED] = Graph(g1.vertices, g1.edges.union(g1.edges.reverse)).cache()
      g2.pregel(initialMsg, Int.MaxValue, EdgeDirection.Out)(vertexProgram, sendMessage, mergeMsg)
    }
  }

  object AccumulationStep {

    type Attr = Map[OriginV, (Betweenness, Distance, NumSP)]
    type Msg = Map[OriginV, Double]

    private[this] def sendMsg[ED: ClassTag](level: Int)(triplet: EdgeContext[Attr, ED, Msg]): Unit = {
      val v = triplet.srcAttr.map {
        case (orig, (bt, distance, sp)) if distance == level && triplet.dstAttr(orig)._2 == level - 1 =>
          orig -> (bt + 1.0) * triplet.dstAttr(orig)._3 / sp
        case (orig, _) => orig -> 0.0
      }.filter(_._2 > 0.0)
      if (v.nonEmpty) {
        triplet.sendToDst(v)
      }
    }

    private[this] def mergeMsg(a: Msg, b: Msg): Msg =
      (a.keySet ++ b.keySet).map(orig => orig -> (a.getOrElse(orig, 0.0) + b.getOrElse(orig, 0.0))).toMap


    def run[ED: ClassTag](graph: Graph[TraversalStep.Attr, ED]): Graph[Double, ED] = {
      // Add results.
      var g = graph.mapVertices { case (v, (newInfo, oldInfo)) =>
        // Note: A smaller Map should be added to the bigger.
        (oldInfo ++ newInfo).map { case (k, (d, p)) => k -> ((0.0, d, p)) }
      }

      val diameter: Distance = g.vertices.map {
        case (vid, attr) => if (attr.isEmpty) 0 else attr.view.map(_._2._2).max
      }.max

      // Decsend from the diameter.
      for (level <- Range(diameter, 1, -1)) {
        val xs: VertexRDD[Msg] = g.aggregateMessages(sendMsg[ED](level), mergeMsg)
        g = g.joinVertices(xs) { case (vid, attr, m) =>
          attr.map { case (v, (bt, d, sp)) =>
            v -> ((m.getOrElse(v, bt), d, sp))
          }
        }
      }

      g.mapVertices { case (v, a) => a.view.map(_._2._1).sum / 2.0 }
    }
  }

}
