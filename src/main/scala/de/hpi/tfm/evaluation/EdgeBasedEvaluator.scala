package de.hpi.tfm.evaluation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.{FactMergeabilityGraph, TupleReference}
import de.hpi.tfm.data.tfmp_input.association.{AssociationIdentifier, AssociationSchema}
import de.hpi.tfm.data.tfmp_input.factLookup.FactLookupTable
import de.hpi.tfm.data.tfmp_input.table.{AbstractTemporalField, TemporalDatabaseTableTrait}
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{FactLineage, SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, ValueTransition}
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG
import de.hpi.tfm.io.{DBSynthesis_IOService, Evaluation_IOService, IOService}

import java.io.PrintWriter

class EdgeBasedEvaluator(subdomain:String, trainGraphConfig: GraphConfig, evaluationGraphConfig:GraphConfig) extends StrictLogging{

  assert(evaluationGraphConfig.timeRangeStart.isAfter(trainGraphConfig.timeRangeEnd))

  val graphFiles = FactMergeabilityGraph.getFieldLineageMergeabilityFiles(subdomain,trainGraphConfig)
  val factLookupTables = scala.collection.mutable.HashMap[AssociationIdentifier, FactLookupTable]()
  val byAssociationID = scala.collection.mutable.HashMap[AssociationIdentifier,  SurrogateBasedSynthesizedTemporalDatabaseTableAssociation]()
  logger.debug("Finished constructor")
  val pr = new PrintWriter(Evaluation_IOService.getEdgeEvaluationFile(subdomain,trainGraphConfig,evaluationGraphConfig))
  pr.println(EdgeEvaluationRow.schema)

  def getValidityAndInterestingness(tr1: TupleReference[Any], tr2: TupleReference[Any]): (Boolean,Boolean) = {
    val originalAndtoCheck = IndexedSeq(tr1,tr2)
      .map(vertex => {
        val surrogateKey = vertex.table.getRow(vertex.rowIndex).keys.head
        //TODO: we need to look up that surrogate key in the bcnf reference table
        val original = getFactLookupTable(vertex.toIDBasedTupleReference.associationID).getCorrespondingValueLineage(surrogateKey)
        val projected = original.projectToTimeRange(evaluationGraphConfig.timeRangeStart,evaluationGraphConfig.timeRangeEnd)
        (original,projected)
      })
    val toCheck = originalAndtoCheck.map(_._2)
    val originals = originalAndtoCheck.map(_._1)
    val res = FactLineage.tryMergeAll(toCheck)
    val interesting = originals.exists(_.lineage.exists{case (t,v) => t.isAfter(trainGraphConfig.timeRangeEnd) && !toCheck.head.isWildcard(v)})
    (res.isDefined,interesting)
  }

  private def getFactLookupTable(id: AssociationIdentifier) = {
    factLookupTables.getOrElseUpdate(id,FactLookupTable.readFromStandardFile(id))
  }

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
        assert(t1 == t2)
        numEqual+=1
      } else{
        numUnEqual+=1
      }
    }
    (numEqual,numUnEqual)
  }

  def getAssociation(associationID: AssociationIdentifier) = {
    byAssociationID.getOrElseUpdate(associationID,SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(associationID))
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
        val (isValid,isInteresting) = getValidityAndInterestingness(tr1,tr2)
        val (numEqual,numUnequal) = getEqualTransitionCount(tr1,tr2)
        val mi = AbstractTemporalField.MUTUAL_INFORMATION(tr1,tr2)
        val newScore = GLOBAL_CONFIG.NEW_TARGET_FUNCTION(tr1,tr2)
        val edgeEvaluationRow = EdgeEvaluationRow(e.tupleReferenceA,e.tupleReferenceB,isValid,isInteresting,numEqual,numUnequal,evidenceCount,mi,newScore)
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
