import org.apache.spark.graphx._

import scala.reflect.ClassTag

class GraphLabeling extends Serializable {
  def run[ED: ClassTag](graph: Graph[Long, ED], maxSteps: Int): VertexRDD[Long] = {
    def vertexProgram(vid: VertexId, attr: Long, message: Map[VertexId, Long]): VertexId = {
      if (message.isEmpty) attr else message.maxBy(_._2)._1
    }

    def sendMessage(e: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, Map[VertexId, Long])] = {
      Iterator((e.srcId, Map(e.dstAttr -> 1L)), (e.dstId, Map(e.srcAttr -> 1L)))
    }

    def mergeMessage(msgMap1: Map[VertexId, Long], msgMap2: Map[VertexId, Long])
    : Map[VertexId, Long] = {
      (msgMap1.keySet ++ msgMap2.keySet).map { key =>
        key -> (msgMap1.getOrElse(key, 0L) + msgMap2.getOrElse(key, 0L))
      }.toMap
    }

    val initialMessage = Map[VertexId, Long]()
    graph.pregel(initialMessage, maxIterations = maxSteps)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage).vertices
  }

}
