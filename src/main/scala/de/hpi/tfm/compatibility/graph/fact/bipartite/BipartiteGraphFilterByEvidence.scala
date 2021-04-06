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
  val startFileIndex = args(5).toInt //in order to allow restart of failed programs!
  val graphConfig = GraphConfig(oldMinEvidence,timeRangeStart,timeRangeEnd)
  logger.debug("Starting to list graph files")
  val allGraphFiles = FactMergeabilityGraph.getFieldLineageMergeabilityFiles(subdomain,graphConfig)
    .toIndexedSeq
  val newGraphConfig = GraphConfig(newMinEvidence,timeRangeStart,timeRangeEnd)
  logger.debug(s"Found ${allGraphFiles.size} graph files")
  (startFileIndex until allGraphFiles.size).foreach(i => {
    val f = allGraphFiles(i)
    if(!f.exists()){
      println(f.getAbsolutePath)
      println("does not exist")
    }
    assert(f.exists())
    val curSubGraph = FactMergeabilityGraph.fromJsonFile(f.getAbsolutePath)
    val newEdges = curSubGraph.edges.filter(e => e.evidence >= newMinEvidence)
    if(newEdges.size>0)
      FactMergeabilityGraph(newEdges,newGraphConfig).writeToStandardFile()
    if(i%100==0)
      logger.debug(s"Finished $i (${100* i / allGraphFiles.size.toDouble}%)")
  })

}
