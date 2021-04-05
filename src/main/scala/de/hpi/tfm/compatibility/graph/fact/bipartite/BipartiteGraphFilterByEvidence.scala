package de.hpi.tfm.compatibility.graph.fact.bipartite

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.association.AssociationGraphEdgeCandidate
import de.hpi.tfm.compatibility.graph.fact.FactMergeabilityGraph
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.tfmp_input.table.nonSketch.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.tfm.io.IOService

import java.time.LocalDate

object BipartiteGraphFilterByEvidence extends App with StrictLogging {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val timeRangeStart = LocalDate.parse(args(2))
  val timeRangeEnd = LocalDate.parse(args(3))
  val oldMinEvidence = 0
  val newMinEvidence = args(4).toInt
  val graphConfig = GraphConfig(oldMinEvidence,timeRangeStart,timeRangeEnd)
  logger.debug("Starting to list graph files")
  val allGraphFiles = FactMergeabilityGraph.getFieldLineageMergeabilityFiles(subdomain,graphConfig)
  val newGraphConfig = GraphConfig(newMinEvidence,timeRangeStart,timeRangeEnd)
  logger.debug(s"Found ${allGraphFiles.size} graph files")
  var processedFiles = 0
  allGraphFiles.foreach(f => {
    val curSubGraph = FactMergeabilityGraph.fromJsonFile(f.getAbsolutePath)
    val newEdges = curSubGraph.edges.filter(e => e.evidence >= newMinEvidence)
    FactMergeabilityGraph(newEdges,newGraphConfig)
      .writeToStandardFile()
    processedFiles+=1
    if(processedFiles%100==0)
      logger.debug(s"Finished $processedFiles (${100* processedFiles / allGraphFiles.size.toDouble}%)")
  })
}
