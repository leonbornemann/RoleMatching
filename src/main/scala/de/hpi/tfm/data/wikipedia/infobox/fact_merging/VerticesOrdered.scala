package de.hpi.tfm.data.wikipedia.infobox.fact_merging

import de.hpi
import de.hpi.tfm
import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.evaluation
import de.hpi.tfm.evaluation.data
import de.hpi.tfm.evaluation.data.{GeneralEdge, IdentifiedFactLineage}

case class VerticesOrdered(vertices:IndexedSeq[IdentifiedFactLineage]) extends JsonWritable[VerticesOrdered] {
  assert((1 until vertices.size)
    .forall(i => vertices(i-1).id < vertices(i).id))
}

object VerticesOrdered extends JsonReadable[VerticesOrdered] {

  def fromEdgeIterator(edgeIterator: hpi.tfm.evaluation.data.GeneralEdge.JsonObjectPerLineFileIterator) = {
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
