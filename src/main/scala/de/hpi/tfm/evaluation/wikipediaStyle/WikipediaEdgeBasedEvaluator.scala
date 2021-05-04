package de.hpi.tfm.evaluation.wikipediaStyle

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.{FactMergeabilityGraph, FactMergeabilityGraphEdge}
import de.hpi.tfm.data.tfmp_input.table.nonSketch.FactLineage
import de.hpi.tfm.evaluation.data.GeneralEdge
import de.hpi.tfm.evaluation.{EdgeEvaluationRow, HoldoutTimeEvaluator}
import de.hpi.tfm.io.Evaluation_IOService

import java.io.{File, PrintWriter}

class WikipediaEdgeBasedEvaluator(subdomain: String,
                                  trainGraphConfig: GraphConfig,
                                  evaluationGraphConfig: GraphConfig,
                                  resultFile:File) extends HoldoutTimeEvaluator(trainGraphConfig,evaluationGraphConfig) with StrictLogging{

  assert(evaluationGraphConfig.timeRangeStart.isAfter(trainGraphConfig.timeRangeEnd))

  val edges = FactMergeabilityGraph.getFieldLineageMergeabilityFiles(subdomain,trainGraphConfig)
    .flatMap(f => {
      logger.debug(s"Loading file $f")
      FactMergeabilityGraph.fromJsonFile(f.getAbsolutePath).edges
    })
  logger.debug("Finished constructor")

  def getRealEdge(e: FactMergeabilityGraphEdge) = {
    val v1 = e.tupleReferenceA.toTupleReference(getAssociation(e.tupleReferenceA.associationID)).getDataTuple.head
    val v2 = e.tupleReferenceB.toTupleReference(getAssociation(e.tupleReferenceB.associationID)).getDataTuple.head
    (v1,v2)
  }

  def evaluate() = {
    val pr = new PrintWriter(resultFile)
    pr.println(EdgeEvaluationRow.schema)
    edges.foreach(e => {
      val realEdge = getRealEdge(e)
      val edgeString1 = "socrata_"+subdomain + e.tupleReferenceA.toString
      val edgeString2 = "socrata_"+subdomain + e.tupleReferenceB.toString
      val v1 = realEdge._1.asInstanceOf[FactLineage].toIdentifiedFactLineage(edgeString1)
      val v2 = realEdge._2.asInstanceOf[FactLineage].toIdentifiedFactLineage(edgeString2)
      val identifiedEdge = GeneralEdge(v1,v2)
      val line = identifiedEdge.toGeneralEdgeStatRow(1,trainGraphConfig)
        .toCSVLine
      pr.println(line)
    })
    pr.close()
  }
}
