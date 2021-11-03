package de.hpi.role_matching.cbrm.evidence_based_weighting.isf

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.data.RoleLineageWithID

import java.io.File

object ISFMapExtraction extends App with StrictLogging {
  logger.debug(s"called with ${args.toIndexedSeq}")
  val inputEdgeFile = args(0)
  val granularityInDays = args(1).toInt
  val resultFile = new File(args(2))
  val edgeIterator = SimpleCompatbilityGraphEdge.iterableFromJsonObjectPerLineFile(inputEdgeFile)
  logger.debug("Finished setting up iterators")
  var count = 0
  var nodes = scala.collection.mutable.HashSet[RoleLineageWithID]()
  edgeIterator.foreach(e => {
    nodes.add(e.v1)
    nodes.add(e.v2)
    count += 1
    if (count % 1000000 == 0)
      logger.debug(s"Done with $count")
  })
  logger.debug("finished loading nodes")
  val hist = RoleLineageWithID.getTransitionHistogramForTFIDFFromVertices(nodes.toSeq, granularityInDays)
  ISFMapStorage(hist.toIndexedSeq).toJsonFile(resultFile)
}
