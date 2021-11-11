package de.hpi.role_matching.cbrm.evidence_based_weighting

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.compatibility_graph.representation.slim.MemoryEfficientCompatiblityGraphWithoutEdgeWeight

import java.io.File
import java.time.LocalDate

object TuningDataExportMain extends App {
  val datasource = args(0)
  GLOBAL_CONFIG.setSettingsForDataSource(datasource)
  val simpleGraphFile = args(1)
  val statFile = new File(args(2))
  val graphResultFile = new File(args(3))
  val trainTimeEnds = args(4).split(";").map(t => LocalDate.parse(t))
  val simpleEdgeIterator = SimpleCompatbilityGraphEdge.iterableFromJsonObjectPerLineFile(simpleGraphFile)
  val graph = MemoryEfficientCompatiblityGraphWithoutEdgeWeight.fromGeneralEdgeIterator(simpleEdgeIterator,GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnds.head,trainTimeEnds.tail)
  val isfMaps = graph.getISFMapsAtEndTimes(trainTimeEnds)
  val counter = new EvidenceBasedWeightingEventCounter(graph,isfMaps,GLOBAL_CONFIG.granularityInDays,statFile,graphResultFile)
  counter.aggregateEventCounts(GLOBAL_CONFIG.granularityInDays,1000000) //we do some sampling so that the tuning experiments (python jupyter notebook) can be conveniently executed on a local machine

}
