package de.hpi.role_matching.evaluation.matching

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.sgcp.{NewSubgraph, RoleMerge, ScoreConfig}
import de.hpi.role_matching.cbrm.sgcp.SparseGraphCliquePartitioningMain.args
import de.hpi.role_matching.cbrm.compatibility_graph.representation.SubGraph
import de.hpi.role_matching.cbrm.compatibility_graph.representation.slim.{MemoryEfficientCompatiblityGraph, MemoryEfficientCompatiblityGraphSet}
import de.hpi.role_matching.cbrm.data.Roleset

import java.io.{File, PrintWriter}
import java.time.LocalDate

object RoleMatchingEvaluationMain extends App with StrictLogging {
  logger.debug(s"Called with ${args.toIndexedSeq}")
  val datasource = args(0)
  GLOBAL_CONFIG.setSettingsForDataSource(datasource)
  val resultDirBruteForceAndGreedy = args(1)
  val resultDirMDMCP = new File(args(2))
  val vertexMappingDirMDMCP = new File(args(3))
  val graphFile = args(4)
  val trainTimeEnd = LocalDate.parse(args(5))
  val scoreConfig = if(args(6)=="MAX_RECALL" || args(6) == "UNWEIGHTED") None else Some(ScoreConfig.fromJsonFile(args(7)))
  val maxRecallSetting = args(6)=="MAX_RECALL"
  val baselineNoWeightSetting = args(6) == "UNWEIGHTED"
  val resultDir = args(7)
  val roleset = Roleset.fromJsonFile(args(8))
  val graphSet = MemoryEfficientCompatiblityGraphSet.fromJsonFile(graphFile)
  val optimizationGraph = (if(scoreConfig.isDefined) graphSet.transformToOptimizationGraph(trainTimeEnd,scoreConfig.get)
  else if(maxRecallSetting) graphSet.getMaxRecallSettingOptimizationGraph(trainTimeEnd,roleset)
  else graphSet.getBaselineNoWeightSetting(trainTimeEnd))
  val mergeFilesFromMDMCP = resultDirMDMCP.listFiles().map(f => (f.getName, f)).toMap
  val partitionVertexFiles = vertexMappingDirMDMCP.listFiles().map(f => (f.getName, f)).toMap
  assert(mergeFilesFromMDMCP.keySet==partitionVertexFiles.keySet)
  new File(resultDir).mkdirs()
  val pr = new PrintWriter(resultDir + "/cliques.csv")
  val prCliquesTruePositivesToReview = new PrintWriter(resultDir + "/cliques_To_Review_True_positives.csv")
  val prCliquesRestToReview = new PrintWriter(resultDir + "/cliques_To_Review_Rest.csv")
  val tableStringPr = new PrintWriter(resultDir + "/tableStrings.txt")
  val prEdges = new PrintWriter(resultDir + "/edges.csv")
  val cliqueAnalyser = new RoleMatchinEvaluator(pr,prCliquesTruePositivesToReview,prCliquesRestToReview,tableStringPr,prEdges, roleset, trainTimeEnd,Some(graphSet), scoreConfig)
  cliqueAnalyser.serializeSchema()
  val mdmcpMerges = mergeFilesFromMDMCP.foreach { case (fname, mf) => {
    val cliquesMDMCP = new MDMCPResultReader(new NewSubgraph(optimizationGraph), mf, partitionVertexFiles(fname)).cliques
    val componentName = fname.split("\\.")(0)
    cliqueAnalyser.addResultTuples(cliquesMDMCP, componentName, "MDMCP")
  }
  }
  new File(resultDirBruteForceAndGreedy).listFiles().foreach(f => {
    val cliquesThisFile = RoleMerge.fromJsonObjectPerLineFile(f.getAbsolutePath)
    val componentName = "-"
    cliqueAnalyser.addResultTuples(cliquesThisFile, componentName, f.getName.split("\\.")(0))
  })
  cliqueAnalyser.printResults()
  pr.close()
  prEdges.close()
  prCliquesTruePositivesToReview.close()
  prCliquesRestToReview.close()
  tableStringPr.close()

}
