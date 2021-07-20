package de.hpi.role_matching.compatibility.graph.representation.slim

import de.hpi.role_matching.compatibility.graph.representation.vertex.IdentifiedFactLineage
import de.hpi.socrata.{JsonReadable, JsonWritable}

case class VertexLookupMap(vertexNamesOrdered: IndexedSeq[String], private val posToLineage:Map[Int,IdentifiedFactLineage]) extends JsonWritable[VertexLookupMap]{
  vertexNamesOrdered.zipWithIndex.foreach(t => assert(posToLineage(t._2).id==t._1))

  val posToFactLineage = posToLineage.map(t => (t._1,t._2.factLineage.toFactLineage))
}
object VertexLookupMap extends JsonReadable[VertexLookupMap]
