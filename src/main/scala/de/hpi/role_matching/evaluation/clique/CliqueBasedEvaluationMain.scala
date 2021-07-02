package de.hpi.role_matching.evaluation.clique

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.compatibility.graph.representation.SubGraph
import de.hpi.role_matching.compatibility.graph.representation.slim.SLimGraph
import de.hpi.role_matching.compatibility.graph.representation.vertex.VerticesOrdered
import de.hpi.role_matching.clique_partitioning.IdentifiedTupleMerge
import de.hpi.role_matching.evaluation.fromWikipedia.CliqueAnalyser

import java.io.{File, PrintWriter}
import java.time.LocalDate

object CliqueBasedEvaluationMain extends App with StrictLogging {
  logger.debug(s"Called with ${args.toIndexedSeq}")
  val mergeDirScala = args(0)
  val mergeDirMDMCP = new File(args(1))
  val mergeDirMappingDir = new File(args(2))
  val graphFile = args(3)
  val verticesOrderedFile = args(4)
  val timeStart = LocalDate.parse(args(5))
  val trainTimeEnd = LocalDate.parse(args(6))
  val timeEnd = LocalDate.parse(args(7))
  GLOBAL_CONFIG.STANDARD_TIME_FRAME_START = timeStart
  GLOBAL_CONFIG.STANDARD_TIME_FRAME_END = timeEnd
  val resultFile = args(8)
  val slimGraph = SLimGraph.fromJsonFile(graphFile)
  val verticesOrdered = VerticesOrdered.fromJsonFile(verticesOrderedFile)
  val mergeFilesFromMDMCP = mergeDirMDMCP.listFiles().map(f => (f.getName, f)).toMap
  val partitionVertexFiles = mergeDirMappingDir.listFiles().map(f => (f.getName, f)).toMap
  //assert(mergeFilesFromMDMCP.keySet==partitionVertexFiles.keySet)
  val pr = new PrintWriter(resultFile)
  val cliqueAnalyser = new CliqueAnalyser(pr, verticesOrdered, trainTimeEnd)
  cliqueAnalyser.serializeSchema()
  val mdmcpMerges = mergeFilesFromMDMCP.foreach { case (fname, mf) => {
    val cliquesMDMCP = new MDMCPResult(new SubGraph(slimGraph.transformToOptimizationGraph), mf, partitionVertexFiles(fname)).cliques
    val componentName = fname.split("\\.")(0)
    cliqueAnalyser.addResultTuples(cliquesMDMCP, componentName, "MDMCP")
  }
  }
  new File(mergeDirScala).listFiles().foreach(f => {
    val cliquesThisFile = IdentifiedTupleMerge.fromJsonObjectPerLineFile(f.getAbsolutePath)
    val componentName = "-"
    cliqueAnalyser.addResultTuples(cliquesThisFile, componentName, f.getName.split("\\.")(0))
  })
  pr.close()
}
