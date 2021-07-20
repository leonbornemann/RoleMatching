package de.hpi.role_matching.compatibility.graph.representation.vertex

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.compatibility.graph.representation.slim.{SlimGraphWithoutWeight, VertexLookupMap}

import java.io.File

object SlimGraphWithoutWeightToLookupMapMain extends App with StrictLogging{
  logger.debug(s"called with ${args.toIndexedSeq}")
  private val lookupFile = new File(args(1))
  val sg = SlimGraphWithoutWeight.fromJsonFile(args(0))
  val ids = sg.verticesOrdered.map(_.id)
  val indicesToLineage = sg
    .verticesOrdered
    .zipWithIndex
    .map(t => (t._2,t._1))
    .toMap
  val vertexLookupMap = VertexLookupMap(ids,indicesToLineage)
  vertexLookupMap.toJsonFile(lookupFile)
}
