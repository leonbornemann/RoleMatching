package de.hpi.role_matching.cbrm.compatibility_graph.representation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.compatibility_graph.representation.slim.MemoryEfficientCompatiblityGraph
import de.hpi.role_matching.cbrm.evidence_based_weighting.EvidenceBasedWeighingScore
import de.hpi.role_matching.cbrm.evidence_based_weighting.isf.ISFMapStorage

import java.io.File
import java.time.LocalDate

object GeneralEdgeGraphToSlimGraph extends App with StrictLogging {
  logger.debug(s"called with ${args.toIndexedSeq}")
  val dataSource = args(0)
  GLOBAL_CONFIG.setSettingsForDataSource(dataSource)
  val simpleEdgeFile = args(1)
  val memoryEfficientGraphFile = args(2)
  val trainTimeEnd = LocalDate.parse(args(3))
  val tfIDFFile = Some(args(4))
  val scoringFunctionThreshold = args(5).toDouble
  val edges = SimpleCompatbilityGraphEdge.iterableFromJsonObjectPerLineFile(simpleEdgeFile)
  //val lineageCount = GeneralEdge.getLineageCount(edges)
  logger.debug("Done loading edges")
  val tfIDF = ISFMapStorage.fromJsonFile(tfIDFFile.get).asMap
  logger.debug("Done loading TF-IDF")
  val scoringFunction = new EvidenceBasedWeighingScore(GLOBAL_CONFIG.granularityInDays, trainTimeEnd, GLOBAL_CONFIG.nonInformativeValues, true, Some(tfIDF), None)
  val graph = MemoryEfficientCompatiblityGraph.fromGeneralEdgeIterator(edges, scoringFunction, scoringFunctionThreshold)
  logger.debug("Done Transforming to MDMCP Graph")
  graph.toJsonFile(new File(memoryEfficientGraphFile))
  logger.debug("Done writing slim graph file")

}
