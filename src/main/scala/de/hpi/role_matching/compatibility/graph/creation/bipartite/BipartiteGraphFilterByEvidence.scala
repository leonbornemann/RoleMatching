package de.hpi.role_matching.compatibility.graph.creation.bipartite

import com.typesafe.scalalogging.StrictLogging
import de.hpi.socrata.io.Socrata_IOService
import de.hpi.role_matching.compatibility.GraphConfig
import de.hpi.role_matching.compatibility.graph.creation.FactMergeabilityGraph

import java.time.LocalDate

object BipartiteGraphFilterByEvidence extends App with StrictLogging {

  Socrata_IOService.socrataDir = args(0)
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
