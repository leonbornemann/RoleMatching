package de.hpi.tfm.data.wikipedia.infobox.fact_merging

import de.hpi.tfm.data.wikipedia.infobox.fact_merging.CliqueMatchExplorationMain.args
import de.hpi.tfm.evaluation.data.GeneralEdge

import java.io.File

object VertexLineageExportMain extends App {
  val graphFile = args(0)
  val vertexFile = new File(args(1))
  val edgeIterator = GeneralEdge.iterableFromJsonObjectPerLineFile(graphFile)
  val vericesOrdered = VerticesOrdered.fromEdgeIterator(edgeIterator)
  vericesOrdered.toJsonFile(vertexFile)
}
