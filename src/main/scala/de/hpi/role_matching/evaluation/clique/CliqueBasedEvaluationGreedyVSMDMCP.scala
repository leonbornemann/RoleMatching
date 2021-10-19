package de.hpi.role_matching.evaluation.clique

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.compatibility.graph.representation.SubGraph
import de.hpi.role_matching.compatibility.graph.representation.slim.{SLimGraph, SlimGraphSet, VertexLookupMap}
import de.hpi.role_matching.compatibility.graph.representation.vertex.VerticesOrdered
import de.hpi.role_matching.clique_partitioning.{NewSubgraph, RoleMerge, ScoreConfig}
import de.hpi.role_matching.evaluation.clique.CliqueBasedEvaluationMain.args

import java.io.{File, PrintWriter}
import java.time.LocalDate

object CliqueBasedEvaluationGreedyVSMDMCP extends App with StrictLogging {
  ScoreConfig(0.1f,1.0f,0.1f,0,-0.1f,-1.0f,364).toJsonFile(new File("lol.txt"))
  assert(false) //still uses slim graph instead of the new data structures
  logger.debug(s"Called with ${args.toIndexedSeq}")
  val mergeDirGreedy = args(0)
  val mergeDir = new File(args(1))
  val mergeDirMappingDir = new File(args(2))
  val graphFile = args(3)
  val vertexLookupFile = args(4)
  val timeStart = LocalDate.parse(args(5))
  val trainTimeEnd = LocalDate.parse(args(6))
  val timeEnd = LocalDate.parse(args(7))
  GLOBAL_CONFIG.STANDARD_TIME_FRAME_START = timeStart
  GLOBAL_CONFIG.STANDARD_TIME_FRAME_END = timeEnd
  val resultDir = args(8)
  val scoreConfig = if(args(9) == "maxRecall") None else Some(ScoreConfig.fromJsonFile(args(9)))
  val slimGraphSet = SlimGraphSet.fromJsonFile(graphFile)
  val vertexlookupMap = VertexLookupMap.fromJsonFile(vertexLookupFile)
  val mergeFilesFromMDMCP = mergeDir.listFiles().map(f => (f.getName, f)).toMap
  val partitionVertexFiles = mergeDirMappingDir.listFiles().map(f => (f.getName, f)).toMap
  //assert(mergeFilesFromMDMCP.keySet==partitionVertexFiles.keySet)
  val pr = new PrintWriter(resultDir + "/cliques.csv")
  val prCliquesTruePositivesToReview = new PrintWriter(resultDir + "/cliques_To_Review_True_positives.csv")
  val prCliquesRestToReview = new PrintWriter(resultDir + "/cliques_To_Review_Rest.csv")
  val tableStringPr = new PrintWriter(resultDir + "/tableStrings.txt")
  val prEdges = new PrintWriter(resultDir + "/edges.csv")
  val cliqueAnalyser = new CliqueAnalyser(pr,prCliquesTruePositivesToReview,prCliquesRestToReview,tableStringPr,prEdges, vertexlookupMap, trainTimeEnd,Some(slimGraphSet),scoreConfig)
  cliqueAnalyser.serializeSchema()
  val mdmcpMerges = mergeFilesFromMDMCP.foreach { case (fname, mf) => {
    val cliquesMDMCP = new MDMCPResult(new NewSubgraph(slimGraphSet.transformToOptimizationGraph(trainTimeEnd,scoreConfig.get)), mf, partitionVertexFiles(fname)).cliques
    val componentName = fname.split("\\.")(0)
    val cliquesGreedy = RoleMerge.fromJsonObjectPerLineFile(mergeDirGreedy + s"/$componentName.json")
    cliqueAnalyser.addResultTuples(cliquesGreedy, componentName, "greedy")
    cliqueAnalyser.addResultTuples(cliquesMDMCP, componentName, "MDMCP")
  }
  }
  pr.close()
  prEdges.close()
  prCliquesTruePositivesToReview.close()
  prCliquesRestToReview.close()
  tableStringPr.close()
}
