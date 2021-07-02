package de.hpi.role_matching.compatibility.graph.representation.vertex

import de.hpi.role_matching.compatibility.graph.representation.simple.GeneralEdge

import java.io.File

object VertexLineageExportMain extends App {
  val graphFile = args(0)
  val vertexFile = new File(args(1))
  val edgeIterator = GeneralEdge.iterableFromJsonObjectPerLineFile(graphFile)
  val vericesOrdered = VerticesOrdered.fromEdgeIterator(edgeIterator)
  vericesOrdered.toJsonFile(vertexFile)
}
