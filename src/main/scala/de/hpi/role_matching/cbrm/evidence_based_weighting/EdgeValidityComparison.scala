package de.hpi.role_matching.cbrm.evidence_based_weighting

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.compatibility_graph.representation.slim.MemoryEfficientCompatiblityGraphWithoutEdgeWeight
import de.hpi.role_matching.cbrm.evidence_based_weighting.TuningDataExportMain.args
import de.hpi.role_matching.evaluation.tuning.EdgeStatRowForTuning

import java.io.File
import java.time.LocalDate

object EdgeValidityComparison extends App {
  println(s"Called with ${args.toIndexedSeq}")
  val datasource = args(0)
  GLOBAL_CONFIG.setSettingsForDataSource(datasource)
  val simpleGraphFile1 = new File(args(1))
  val simpleGraphFile2 = new File(args(2))
  val trainTimeEnd = LocalDate.parse(args(3))
  val edges1 = SimpleCompatbilityGraphEdge.iterableFromJsonObjectPerLineDir(simpleGraphFile1)
    .toIndexedSeq
    .map(e => (e.getEdgeID,e))
    .toMap
  println("Read file 1")
  val edges2 = SimpleCompatbilityGraphEdge.iterableFromJsonObjectPerLineDir(simpleGraphFile2)
    .toIndexedSeq
    .map(e => (e.getEdgeID,e))
    .toMap
  println("Read file 2")
  println(s"edges 1: ${edges1.size} edges 2: ${edges2.size} intersection: ${edges1.keySet.intersect(edges2.keySet).size}")
  //check common edges:
  val edgesUniqueTo2 = edges2.keySet.diff(edges1.keySet)
  println("edgesUniqueTo2: "+ edgesUniqueTo2.size)
  val res = edgesUniqueTo2.map(e => {
    val edge = edges2(e)
    val rl1 = edge.v1.roleLineage.toRoleLineage
    val rl2 = edge.v2.roleLineage.toRoleLineage
    val remainsValid = rl1.tryMergeWithConsistent(rl2).isDefined
    val evidenceInTrainPhase = rl1.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd).getOverlapEvidenceCount(rl2.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd))
    (remainsValid,evidenceInTrainPhase)
  })
  val withEvidence = res.filter(t => t._2>0)
  println("With Evidence:" + withEvidence.size)
//  edges1.keySet.intersect(edges2.keySet).foreach(k => {
//    val e1 = edges1(k)
//    val e2 = edges2(k)
//    val statRow:EventOccurrenceStatistics = e1.eventOccurrences(trainTimeEnd)
//    val lol = EdgeStatRowForTuning(e1,statRow,7)
//  })
}
