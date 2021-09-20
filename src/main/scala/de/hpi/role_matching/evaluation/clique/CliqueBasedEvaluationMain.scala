package de.hpi.role_matching.evaluation.clique

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.clique_partitioning.SparseGraphCliquePartitioningMain.args
import de.hpi.role_matching.compatibility.graph.representation.SubGraph
import de.hpi.role_matching.compatibility.graph.representation.slim.{SLimGraph, SlimGraphSet, VertexLookupMap}
import de.hpi.role_matching.compatibility.graph.representation.vertex.VerticesOrdered
import de.hpi.role_matching.clique_partitioning.{NewSubgraph, RoleMerge, ScoreConfig}

import java.io.{File, PrintWriter}
import java.time.LocalDate

object CliqueBasedEvaluationMain extends App with StrictLogging {
  logger.debug(s"Called with ${args.toIndexedSeq}")
  val mergeDirScala = args(0)
  val mergeDirMDMCP = new File(args(1))
  val mergeDirMappingDir = new File(args(2))
  val graphFile = args(3)
  val lookupMapFile = new File(args(4))
  val source = args(5)
  val trainTimeEnd = LocalDate.parse(args(6))
  val scoreConfig = if(args(7)=="max_recall" || args(7) == "baselineNoWeight") None else Some(ScoreConfig.fromJsonFile(args(7)))
  val maxRecallSetting = args(7)=="max_recall"
  val baselineNoWeightSetting = args(7) == "baselineNoWeight"
  GLOBAL_CONFIG.setDatesForDataSource(source)
  val resultDir = args(8)
  val graphSet = SlimGraphSet.fromJsonFile(graphFile)
  val vertexLookupMap = VertexLookupMap.fromJsonFile(lookupMapFile.getAbsolutePath)
  val optimizationGraph = (if(scoreConfig.isDefined) graphSet.transformToOptimizationGraph(trainTimeEnd,scoreConfig.get)
    else if(maxRecallSetting) graphSet.getMaxRecallSettingOptimizationGraph(trainTimeEnd,vertexLookupMap)
    else graphSet.getBaselineNoWeightSetting(trainTimeEnd))
  val mergeFilesFromMDMCP = mergeDirMDMCP.listFiles().map(f => (f.getName, f)).toMap
  val partitionVertexFiles = mergeDirMappingDir.listFiles().map(f => (f.getName, f)).toMap
  assert(mergeFilesFromMDMCP.keySet==partitionVertexFiles.keySet)
  new File(resultDir).mkdirs()
  val pr = new PrintWriter(resultDir + "/cliques.csv")
  val prEdges = new PrintWriter(resultDir + "/edges.csv")
  val cliqueAnalyser = new CliqueAnalyser(pr,prEdges, vertexLookupMap, trainTimeEnd,graphSet, scoreConfig)
  cliqueAnalyser.serializeSchema()
  val mdmcpMerges = mergeFilesFromMDMCP.foreach { case (fname, mf) => {
    val cliquesMDMCP = new MDMCPResult(new NewSubgraph(optimizationGraph), mf, partitionVertexFiles(fname)).cliques
    val componentName = fname.split("\\.")(0)
    cliqueAnalyser.addResultTuples(cliquesMDMCP, componentName, "MDMCP")
  }
  }
  new File(mergeDirScala).listFiles().foreach(f => {
    val cliquesThisFile = RoleMerge.fromJsonObjectPerLineFile(f.getAbsolutePath)
    val componentName = "-"
    cliqueAnalyser.addResultTuples(cliquesThisFile, componentName, f.getName.split("\\.")(0))
  })
  pr.close()
  prEdges.close()
}
