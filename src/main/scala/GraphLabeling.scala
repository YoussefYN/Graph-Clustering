import org.apache.spark.graphx._

import scala.reflect.ClassTag

class GraphLabeling extends Serializable {
  def run[ED: ClassTag](graph: Graph[Long, ED], maxSteps: Int): VertexRDD[Long] = {

    /*
    * parameters:
    *   vid: Id of the vertex
    *   attr: The current state of the vertex
    *   message: The aggregated message sent by the neighbouring nodes.
    * Returns:
    *   The new state of the vertex
    * */
    def vertexProgram(vid: VertexId, attr: Long, message: Map[VertexId, Long]): Long = {
      if (message.isEmpty) attr else message.maxBy(_._2)._1
    }

    /*
    * parameters:
    *   e: Edge
    *
    * Returns:
    *   Iterator of the messages that will be sent across this edges.
    * */
    def sendMessage(e: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, Map[VertexId, Long])] = {
      Iterator((e.srcId, Map(e.dstAttr -> 1L)), (e.dstId, Map(e.srcAttr -> 1L)))
    }

    /*
    * parameters:
    *   msgMap1 & msgMap2: Two messages
    * Returns:
    *   The merged message.
    * */
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
