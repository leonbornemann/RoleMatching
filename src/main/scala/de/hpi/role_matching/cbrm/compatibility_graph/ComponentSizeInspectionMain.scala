package de.hpi.role_matching.cbrm.compatibility_graph

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.slim.{MemoryEfficientCompatiblityGraphSet, MemoryEfficientCompatiblityGraphWithoutEdgeWeight}
import de.hpi.role_matching.cbrm.sgcp.ScoreConfig
import de.hpi.role_matching.cbrm.sgcp.SparseGraphCliquePartitioningMain.args

import java.io.File
import java.time.LocalDate

object ComponentSizeInspectionMain extends App with StrictLogging{
  val inputGraphDir = new File(args(0))
  val dsNames = args(1).split(";")
  val trainTimeEnds = args(2).split(";").map(LocalDate.parse(_))
  val scoreConfig = ScoreConfig(0.0f,1,1,1,1,1,1)
  val resultDir = args(2)
  assert(trainTimeEnds.size==dsNames.size)
  dsNames.zip(trainTimeEnds).foreach{ case (dsName,trainTimeEnd) => {
    logger.debug(s"Processing $dsName")
    val inputGraphFile = new File(inputGraphDir.getAbsolutePath + s"/$dsName/$dsName.json")
    val resultFile = new File(resultDir + s"/$dsName.json")
    val graph = MemoryEfficientCompatiblityGraphSet.fromJsonFile(inputGraphFile.getAbsolutePath)
      .transformToOptimizationGraph(trainTimeEnd,scoreConfig)
    val componentSizePrinter = new ComponentSizerPrinter(graph,resultFile)
    componentSizePrinter.runComponentWiseOptimization()
  }}

}
