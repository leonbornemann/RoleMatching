package de.hpi.tfm.data.wikipedia.infobox.fact_merging

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.evaluation.data.{GeneralEdge, IdentifiedFactLineage, SLimGraph}
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG
import de.hpi.tfm.fact_merging.metrics.{MultipleEventWeightScore, TFIDFMapStorage, TFIDFWeightingVariant}
import de.hpi.tfm.io.IOService

import java.io.{File, PrintWriter}
import java.time.LocalDate

object GraphFilterMain extends App with StrictLogging {
  logger.debug(s"Called with ${args.toIndexedSeq}")
  val matchFile = new File(args(0))
  val filteredGraphResultDir = new File(args(1))
  val filteredVerticesOrderedResultDir = new File(args(2))
  val filteredTFIDFResultDir = new File(args(3))
  Seq(filteredGraphResultDir,filteredVerticesOrderedResultDir,filteredTFIDFResultDir).foreach(_.mkdirs())
  val timeStart = LocalDate.parse(args(4))
  val endDateTrainPhases = args(5).split(";").map(LocalDate.parse(_)).toIndexedSeq
  val timeEnd = LocalDate.parse(args(6))
  val granularityInDays = args(7).toInt
  val scoringFunctionThreshold = 10
  val scoringFunction = new DummyScoringFunction()
  IOService.STANDARD_TIME_FRAME_START = timeStart
  IOService.STANDARD_TIME_FRAME_END = timeEnd
  val outputs = endDateTrainPhases
    .map(ld => (ld,new PrintWriter(filteredGraphResultDir.getAbsolutePath + s"/$ld.json")))
  val vertices = endDateTrainPhases
    .map(ld => (ld,collection.mutable.HashMap[String,IdentifiedFactLineage]()))
    .toMap
  var nProcessed = 0
  val edges = GeneralEdge.iterableFromJsonObjectPerLineFile(matchFile.getAbsolutePath).foreach(edge => {
    outputs.foreach{case (trainEnd,pr) => {
      //if still valid serialize this edge!
      val v1Train = edge.v1.factLineage.toFactLineage.projectToTimeRange(timeStart,trainEnd)
      val v2Train = edge.v2.factLineage.toFactLineage.projectToTimeRange(timeStart,trainEnd)
      if(v1Train.tryMergeWithConsistent(v2Train).isDefined){
        //val edgeNew = SLimGraph.getEdgeOption(edge,scoringFunction,0.0)
        edge.appendToWriter(pr,false,true)
        vertices(trainEnd).put(edge.v1.id,edge.v1)
        vertices(trainEnd).put(edge.v2.id,edge.v2)
      }
    }}
    nProcessed+=1
    if(nProcessed%100000==0){
      logger.debug(s"Finished $nProcessed")
    }
  })
  outputs.foreach(_._2.close())
  logger.debug("Done with Filtered Graphs")
  //write
  vertices.foreach{case (ld,vertexMap) => {
    val sorted = vertexMap.values.toIndexedSeq.sortBy(_.id)
    val verticesOrdered = VerticesOrdered(sorted)
    val verticesFile = new File(s"${filteredVerticesOrderedResultDir.getAbsolutePath}/$ld.json")
    logger.debug(s"Serializing vertices ordered to $verticesFile")
    verticesOrdered.toJsonFile(verticesFile)
    val tfIDF = IdentifiedFactLineage.getTransitionHistogramForTFIDFFromVertices(verticesOrdered.vertices,granularityInDays)
    val tfIDFFile = new File(s"${filteredTFIDFResultDir.getAbsolutePath}/$ld.json")
    logger.debug(s"Serializing vertices ordered to $tfIDFFile")
    TFIDFMapStorage(tfIDF.toIndexedSeq).toJsonFile(tfIDFFile)
  }}
  logger.debug("Done Serializing vertices and TF-IDF")
  assert(false) //TODO:also needs to create TF-IDF

}
