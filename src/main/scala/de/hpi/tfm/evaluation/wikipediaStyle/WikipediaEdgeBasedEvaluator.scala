package de.hpi.tfm.evaluation.wikipediaStyle

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.{FactMergeabilityGraph, FactMergeabilityGraphEdge}
import de.hpi.tfm.data.tfmp_input.table.nonSketch.FactLineage
import de.hpi.tfm.evaluation.HoldoutTimeEvaluator
import de.hpi.tfm.evaluation.data.{GeneralEdge, IdentifiedFactLineage}
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG

import java.io.{File, PrintWriter}

class WikipediaEdgeBasedEvaluator(subdomain: String,
                                  trainGraphConfig: GraphConfig,
                                  evaluationGraphConfig: GraphConfig,
                                  resultFileJson:File,
                                  resultFileStats:File) extends HoldoutTimeEvaluator(trainGraphConfig,evaluationGraphConfig) with StrictLogging{

  assert(evaluationGraphConfig.timeRangeStart.isAfter(trainGraphConfig.timeRangeEnd))

  val edges = FactMergeabilityGraph.getFieldLineageMergeabilityFiles(subdomain,trainGraphConfig)
    .flatMap(f => {
      logger.debug(s"Loading file $f")
      FactMergeabilityGraph.fromJsonFile(f.getAbsolutePath).edges
    })
    .toIndexedSeq
  val edgesGeneral = edges.map(e => {
    getGeneralEdge(e)
  })
  val hist = GeneralEdge.getTransitionHistogramForTFIDF(edgesGeneral,1)
  val count = GeneralEdge.getLineageCount(edgesGeneral)
  logger.debug("Finished constructor")

  def getRealEdge(e: FactMergeabilityGraphEdge) = {
    val v1 = e.tupleReferenceA.toTupleReference(getAssociation(e.tupleReferenceA.associationID))
    //val v1 = tupleReference1.getDataTuple.head
    val v2 = e.tupleReferenceB.toTupleReference(getAssociation(e.tupleReferenceB.associationID))
    //val v2 = tupleReference2.getDataTuple.head
    val originals = referencesToOriginal(IndexedSeq(v1,v2))
    (originals(0),originals(1))
  }

  def getGeneralEdge(e: FactMergeabilityGraphEdge) ={
    val realEdge = getRealEdge(e)
    val edgeString1 = IdentifiedFactLineage.getIDString(subdomain,e.tupleReferenceA)
    val edgeString2 = IdentifiedFactLineage.getIDString(subdomain,e.tupleReferenceB)
    val v1 = realEdge._1.asInstanceOf[FactLineage].toIdentifiedFactLineage(edgeString1)
    val v2 = realEdge._2.asInstanceOf[FactLineage].toIdentifiedFactLineage(edgeString2)
    GeneralEdge(v1,v2)
  }

  def evaluate() = {
    val prStats = new PrintWriter(resultFileStats)
    val prJson = new PrintWriter(resultFileJson)
    edgesGeneral
      .zipWithIndex
      .foreach{case (identifiedEdge,i) => {
        identifiedEdge.appendToWriter(prJson,false,true)
        if(i==0){
          prStats.println(identifiedEdge.toGeneralEdgeStatRow(1,trainGraphConfig,GLOBAL_CONFIG.nonInformativeValues,hist,count).getSchema.mkString(","))
        }
        val line = identifiedEdge.toGeneralEdgeStatRow(1,trainGraphConfig,GLOBAL_CONFIG.nonInformativeValues,hist,count)
          .toCSVLine
        prStats.println(line)
    }}
    prStats.close()
    prJson.close()
  }
}
