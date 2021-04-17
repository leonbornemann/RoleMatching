package de.hpi.tfm.evaluation

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.fact_merging.optimization.TupleMerge
import de.hpi.tfm.io.Evaluation_IOService

import java.io.PrintWriter

case class CliqueBasedEvaluator(subdomain: String, optimizationMethodName: String, targetFunctionName: String, trainGraphConfig: GraphConfig, evaluationGraphConfig: GraphConfig) extends HoldoutTimeEvaluator(trainGraphConfig,evaluationGraphConfig){

  val mergeFiles = TupleMerge.getStandardObjectPerLineFiles(subdomain,optimizationMethodName,targetFunctionName)

  def getNumValidEdges(references: IndexedSeq[TupleReference[Any]]) = {
    val originals = references
      .map(vertex => {
        val surrogateKey = vertex.table.getRow(vertex.rowIndex).keys.head
        //TODO: we need to look up that surrogate key in the bcnf reference table
        getFactLookupTable(vertex.toIDBasedTupleReference.associationID).getCorrespondingValueLineage(surrogateKey).projectToTimeRange(evaluationGraphConfig.timeRangeStart,evaluationGraphConfig.timeRangeEnd)
      })
    var numValid = 0
    var numEdges = 0
    for(i <- 0 until originals.size){
      for(j <- (i+1) until originals.size){
        numEdges+=1
        if(originals(i).tryMergeWithConsistent(originals(j)).isDefined) numValid +=1
      }
    }
    (numValid,numEdges)
  }

  def evaluate() = {
    val pr = new PrintWriter(Evaluation_IOService.getCliqueEvaluationFile(subdomain,optimizationMethodName,targetFunctionName,trainGraphConfig))
    pr.println(CliqueEvaluationRow.schema)
    mergeFiles.foreach(f => {
      val merges = TupleMerge.fromJsonObjectPerLineFile(f.getAbsolutePath)
      merges.foreach(tm => {
        val vertices = tm.clique.toIndexedSeq.sortBy(_.associationID.compositeID)
        val tupleReferenceVertices = tm.clique.toIndexedSeq.map(tridb => tridb.toTupleReference(getAssociation(tridb.associationID)))
        val (remainsValid,hasChangeAfterTrainPeriod) = getValidityAndInterestingness(tupleReferenceVertices)
        val numVerticesWithChangeAfterTrainPeriod = getNumVerticesWithChangeAfterTrainPeriod(tupleReferenceVertices)
        val (numValidEdges,numEdgesInClique) = getNumValidEdges(tupleReferenceVertices)
        val row = CliqueEvaluationRow(optimizationMethodName,targetFunctionName,f.getAbsolutePath,vertices,vertices.size,numEdgesInClique,remainsValid,numValidEdges,numVerticesWithChangeAfterTrainPeriod,tm.score,"All Pair SUM")
        pr.println(row.toCSVRowString())
      })
    })
    pr.close()
  }




}
