package de.hpi.tfm.evaluation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.{FactMergeabilityGraph, TupleReference}
import de.hpi.tfm.data.tfmp_input.table.AbstractTemporalField
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{FactLineage, ValueTransition}
import de.hpi.tfm.io.{Evaluation_IOService, IOService}

import java.io.PrintWriter

class EdgeBasedEvaluator(subdomain:String, trainGraphConfig: GraphConfig, evaluationGraphConfig:GraphConfig) extends HoldoutTimeEvaluator(trainGraphConfig,evaluationGraphConfig) with StrictLogging{

  assert(evaluationGraphConfig.timeRangeStart.isAfter(trainGraphConfig.timeRangeEnd))

  val graphFiles = FactMergeabilityGraph.getFieldLineageMergeabilityFiles(subdomain,trainGraphConfig)
  logger.debug("Finished constructor")
  val pr = new PrintWriter(Evaluation_IOService.getEdgeEvaluationFile(subdomain,trainGraphConfig,evaluationGraphConfig))
  pr.println(EdgeEvaluationRow.schema)

  def getEqualTransitionCount(tr1: TupleReference[Any], tr2: TupleReference[Any]):(Int,Int) = {
    val vl1 = tr1.getDataTuple.head
    val vl2 = tr2.getDataTuple.head
    var numEqual = 0
    var numUnEqual = 0
    val standardtimerange = IOService.STANDARD_TIME_RANGE
    for(i <- 1 until standardtimerange.size){
      val t1 = ValueTransition(vl1.valueAt(standardtimerange(i-1)),vl1.valueAt(standardtimerange(i)))
      val t2 = ValueTransition(vl2.valueAt(standardtimerange(i-1)),vl2.valueAt(standardtimerange(i)))
      if(!FactLineage.isWildcard(t1.prev) && !FactLineage.isWildcard(t1.after) && !FactLineage.isWildcard(t2.prev) && !FactLineage.isWildcard(t2.after)){
        if(t1!=t2){
          println(t1)
          println(t2)
          val mergeRes = vl1.tryMergeWithConsistent(vl2)
          println(mergeRes)
          println(FactLineage.isWildcard())
        }
        assert(t1 == t2)
        numEqual+=1
      } else{
        numUnEqual+=1
      }
    }
    (numEqual,numUnEqual)
  }

  def evaluate() = {
    val totalfileCount = graphFiles.size
    var fileCount = 0
    graphFiles.foreach(f => {
      fileCount +=1
      logger.debug(s"Processing ${f} ($fileCount / $totalfileCount)")
      val g = FactMergeabilityGraph.fromJsonFile(f.getAbsolutePath)
      val totalEdgeCount = g.edges.size
      var processedEdges = 0
      g.edges.foreach(e => {
        val tr1 = e.tupleReferenceA.toTupleReference(getAssociation(e.tupleReferenceA.associationID))
        val tr2 = e.tupleReferenceB.toTupleReference(getAssociation(e.tupleReferenceB.associationID))
        val evidenceCount = tr1.getDataTuple.head.getOverlapEvidenceCount(tr2.getDataTuple.head)
        val (isValid,hasCHangeAfterTrainPeriod) = getValidityAndInterestingness(IndexedSeq(tr1,tr2))
        val dateOfChange = getEarliestPointInTimeOfRealChangeAfterTrainPeriod(IndexedSeq(tr1,tr2))
        val (numEqual,numUnequal) = getEqualTransitionCount(tr1,tr2)
        val mi = AbstractTemporalField.mutualInformation(tr1,tr2)
        val newScore = AbstractTemporalField.multipleEventWeightScore(tr1,tr2)
        val numDaysUntilRealChangeAfterTrainPeriod = if(dateOfChange.isDefined) dateOfChange.get.toEpochDay - IOService.STANDARD_TIME_FRAME_END.toEpochDay  else -1
        val edgeEvaluationRow = EdgeEvaluationRow(e.tupleReferenceA,e.tupleReferenceB,isValid,hasCHangeAfterTrainPeriod,numDaysUntilRealChangeAfterTrainPeriod.toInt,numEqual,numUnequal,evidenceCount,mi,newScore)
        pr.println(edgeEvaluationRow.toCSVRow)
        processedEdges +=1
        if(processedEdges % 100==0){
          logger.debug(s"Processed $processedEdges / $totalEdgeCount (${100*processedEdges / totalEdgeCount.toDouble}%)")
        }
      })
    })
    pr.close()
  }
}
