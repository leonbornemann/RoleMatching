package de.hpi.role_matching.cbrm.compatibility_graph.representation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.compatibility_graph.representation.slim.MemoryEfficientCompatiblityGraph
import de.hpi.role_matching.cbrm.evidence_based_weighting.EvidenceBasedWeighingScore
import de.hpi.role_matching.cbrm.evidence_based_weighting.isf.ISFMapStorage

import java.io.File
import java.time.LocalDate

object GeneralEdgeGraphToSlimGraph extends App with StrictLogging {
  logger.debug(s"called with ${args.toIndexedSeq}")
  //  def scaleInterpolation(x: Double, a: Double, b: Double, c: Double, d: Double) = {
  //    val y = (d-c)*(x-a) / (b-a) +c
  //    assert(y >=c && y <= d)
  //    y
  //  }
  //
  //  val a = List.fill(100)(Random.nextDouble()*2 - 1.0)
  //  val res = a.map(x => (x,scaleInterpolation(x,-1.0,1.0,10,20)))
  //  res.foreach(println)
  //
  //  val input = Source.fromFile("/home/leon/data/dataset_versioning/optimization/MDMCP/instance/gauss500-100-3.txt")
  //    .getLines()
  //    .toIndexedSeq
  //    .map(s => s.split("\\s+").size)
  //  println(input == Seq(1) ++ (1 to 500).reverse)
  //  input.foreach(println)
  val generalEdgeFile = args(0)
  val SlimGraphFile = args(1)
  val TIMESTAMP_RESOLUTION_IN_DAYS = args(2).toInt
  val timeStart = LocalDate.parse(args(3))
  val trainTimeEnd = LocalDate.parse(args(4))
  val timeEnd = LocalDate.parse(args(5))
  val scoringFunctionThreshold = args(6).toDouble //0.460230 for politics for this score
  val tfIDFFile = Some(args(7))
  GLOBAL_CONFIG.STANDARD_TIME_FRAME_START = timeStart
  GLOBAL_CONFIG.STANDARD_TIME_FRAME_END = timeEnd
  val edges = SimpleCompatbilityGraphEdge.iterableFromJsonObjectPerLineFile(generalEdgeFile)
  //val lineageCount = GeneralEdge.getLineageCount(edges)
  logger.debug("Done loading edges")
  val tfIDF = ISFMapStorage.fromJsonFile(tfIDFFile.get).asMap
  logger.debug("Done loading TF-IDF")
  val scoringFunction = new EvidenceBasedWeighingScore(TIMESTAMP_RESOLUTION_IN_DAYS, trainTimeEnd, GLOBAL_CONFIG.nonInformativeValues, true, Some(tfIDF), None)
  val graph = MemoryEfficientCompatiblityGraph.fromGeneralEdgeIterator(edges, scoringFunction, scoringFunctionThreshold)
  logger.debug("Done Transforming to MDMCP Graph")
  graph.toJsonFile(new File(SlimGraphFile))
  logger.debug("Done writing slim graph file")

}
