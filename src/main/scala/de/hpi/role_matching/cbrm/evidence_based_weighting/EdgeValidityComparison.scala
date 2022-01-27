package de.hpi.role_matching.cbrm.evidence_based_weighting

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.compatibility_graph.representation.slim.MemoryEfficientCompatiblityGraphWithoutEdgeWeight
import de.hpi.role_matching.cbrm.evidence_based_weighting.TuningDataExportMain.args

import java.io.File
import java.time.LocalDate

object EdgeValidityComparison extends App {
  println(s"Called with ${args.toIndexedSeq}")
  val datasource = args(0)
  GLOBAL_CONFIG.setSettingsForDataSource(datasource)
  val simpleGraphFile1 = new File(args(1))
  val simpleGraphFile2 = new File(args(2))
  val trainTimeEnds = args(3).split(";").map(t => LocalDate.parse(t))
  val edges1 = SimpleCompatbilityGraphEdge.fromJsonObjectPerLineFile(simpleGraphFile1.getAbsolutePath)
    .map(e => (e.getEdgeID,e))
    .toMap
  println("Read file 1")
  val edges2 = SimpleCompatbilityGraphEdge.fromJsonObjectPerLineFile(simpleGraphFile1.getAbsolutePath)
    .map(e => (e.getEdgeID,e))
    .toMap
  println("Read file 2")
  println(s"edges 1: ${edges1.size} edges 2: ${edges2.size} intersection: ${edges1.keySet.intersect(edges2.keySet)}")
  //check common edges:
  edges1.keySet.intersect(edges2.keySet).foreach(k => {

  })
}
