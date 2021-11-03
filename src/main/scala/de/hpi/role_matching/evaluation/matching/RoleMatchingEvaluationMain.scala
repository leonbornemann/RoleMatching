package de.hpi.role_matching.evaluation.matching

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.sgcp.{NewSubgraph, RoleMerge, ScoreConfig}
import de.hpi.role_matching.cbrm.sgcp.SparseGraphCliquePartitioningMain.args
import de.hpi.role_matching.cbrm.compatibility_graph.representation.SubGraph
import de.hpi.role_matching.cbrm.compatibility_graph.representation.slim.{MemoryEfficientCompatiblityGraph, MemoryEfficientCompatiblityGraphSet}
import de.hpi.role_matching.cbrm.compatibility_graph.representation.vertex.VerticesOrdered
import de.hpi.role_matching.cbrm.data.Roleset
import de.hpi.role_matching.clique_partitioning.RoleMerge

import java.io.{File, PrintWriter}
import java.time.LocalDate

object RoleMatchingEvaluationMain extends App with StrictLogging {
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
  val redoGreedyOnly = args(9).toBoolean
  val graphSet = MemoryEfficientCompatiblityGraphSet.fromJsonFile(graphFile)
  val vertexLookupMap = Roleset.fromJsonFile(lookupMapFile.getAbsolutePath)
  if(!redoGreedyOnly){
    val optimizationGraph = (if(scoreConfig.isDefined) graphSet.transformToOptimizationGraph(trainTimeEnd,scoreConfig.get)
    else if(maxRecallSetting) graphSet.getMaxRecallSettingOptimizationGraph(trainTimeEnd,vertexLookupMap)
    else graphSet.getBaselineNoWeightSetting(trainTimeEnd))
    val mergeFilesFromMDMCP = mergeDirMDMCP.listFiles().map(f => (f.getName, f)).toMap
    val partitionVertexFiles = mergeDirMappingDir.listFiles().map(f => (f.getName, f)).toMap
    assert(mergeFilesFromMDMCP.keySet==partitionVertexFiles.keySet)
    new File(resultDir).mkdirs()
    val pr = new PrintWriter(resultDir + "/cliques.csv")
    val prCliquesTruePositivesToReview = new PrintWriter(resultDir + "/cliques_To_Review_True_positives.csv")
    val prCliquesRestToReview = new PrintWriter(resultDir + "/cliques_To_Review_Rest.csv")
    val tableStringPr = new PrintWriter(resultDir + "/tableStrings.txt")
    val prEdges = new PrintWriter(resultDir + "/edges.csv")
    val cliqueAnalyser = new RoleMatchinEvaluator(pr,prCliquesTruePositivesToReview,prCliquesRestToReview,tableStringPr,prEdges, vertexLookupMap, trainTimeEnd,Some(graphSet), scoreConfig)
    cliqueAnalyser.serializeSchema()
    val mdmcpMerges = mergeFilesFromMDMCP.foreach { case (fname, mf) => {
      val cliquesMDMCP = new MDMCPResultReader(new NewSubgraph(optimizationGraph), mf, partitionVertexFiles(fname)).cliques
      val componentName = fname.split("\\.")(0)
      cliqueAnalyser.addResultTuples(cliquesMDMCP, componentName, "MDMCP")
    }
    }
    new File(mergeDirScala).listFiles().foreach(f => {
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
  } else {
    val pr = new PrintWriter(resultDir + "/cliquesGreedyNew.csv")
    val prCliquesTruePositivesToReview = new PrintWriter(resultDir + "/cliques_To_Review_True_positives.csv")
    val prCliquesRestToReview = new PrintWriter(resultDir + "/cliques_To_Review_Rest.csv")
    val tableStringPr = new PrintWriter(resultDir + "/tableStrings.txt")
    val prEdges = new PrintWriter(resultDir + "/edgesGreedyNew.csv")
    val f = new File(mergeDirScala + "/greedyLargeVertexCountResult.json")
    val cliquesThisFile = RoleMerge.fromJsonObjectPerLineFile(f.getAbsolutePath)
    val componentName = "-"
    val cliqueAnalyser = new RoleMatchinEvaluator(pr,prCliquesTruePositivesToReview,prCliquesRestToReview,tableStringPr,prEdges, vertexLookupMap, trainTimeEnd,Some(graphSet), scoreConfig)
    cliqueAnalyser.serializeSchema()
    cliqueAnalyser.addResultTuples(cliquesThisFile, componentName, f.getName.split("\\.")(0))
    pr.close()
    prEdges.close()
    prCliquesTruePositivesToReview.close()
    prCliquesRestToReview.close()
    tableStringPr.close()
  }

}
