package de.hpi.role_matching.compatibility.graph.representation.vertex

import de.hpi.socrata.{JsonReadable, JsonWritable}
import de.hpi.role_matching.compatibility.graph.representation.simple.GeneralEdge

case class VerticesOrdered(vertices:IndexedSeq[IdentifiedFactLineage]) extends JsonWritable[VerticesOrdered] {
  assert((1 until vertices.size)
    .forall(i => vertices(i-1).id < vertices(i).id))
}

object VerticesOrdered extends JsonReadable[VerticesOrdered] {

  def fromEdgeIterator(edgeIterator:GeneralEdge.JsonObjectPerLineFileIterator) = {
    val vertices = collection.mutable.HashMap[String,IdentifiedFactLineage]()
    edgeIterator.foreach(e => {
      vertices.put(e.v1.id,e.v1)
      vertices.put(e.v2.id,e.v2)
    })
    VerticesOrdered(vertices
      .toIndexedSeq
      .sortBy(_._1)
      .map(_._2))
  }

}
