package de.hpi.tfm.evaluation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.graph.fact.{FactMergeabilityGraph, TupleReference}
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.tfmp_input.factLookup.FactLookupTable
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{FactLineage, SurrogateBasedSynthesizedTemporalDatabaseTableAssociation}
import de.hpi.tfm.evaluation.FieldLineageMergeEvaluationMain.{factLookupTables, tables}
import de.hpi.tfm.evaluation.OldFactLineageStatistics.subdomain
import de.hpi.tfm.io.IOService

class OldFactLineageStatisticGatherer(subdomain:String) extends StrictLogging{

  val connectedComponentFiles = FactMergeabilityGraph.getAllConnectedComponentFiles(subdomain)
  var totalEdgeCount:Long = 0
  val associations = AssociationIdentifier.loadAllAssociationsWithChanges()
  val factLookupTables = tables
    .map(id => (id,FactLookupTable.readFromStandardFile(id)))
    .toMap
  val byAssociationID = tables
    .map(id => (id,SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(id)))
    .toMap
  //all
  val evidenceCountHistogram = scala.collection.mutable.HashMap[Int,Int]()
  //valid
  val validEvidenceCountHistogram = scala.collection.mutable.HashMap[Int,Int]()
  //valid and interesting
  val validAndInterestingEvidenceCountHistogram = scala.collection.mutable.HashMap[Int,Int]()


  def getValidityAndInterestingness(tr1: TupleReference[Any], tr2: TupleReference[Any]): (Boolean,Boolean) = {
    val toCheck = IndexedSeq(tr1,tr2)
      .map(vertex => {
        val surrogateKey = vertex.table.getRow(vertex.rowIndex).keys.head
        //TODO: we need to look up that surrogate key in the bcnf reference table
        val vl = factLookupTables(vertex.toIDBasedTupleReference.associationID).getCorrespondingValueLineage(surrogateKey)
        vl
      })
    val res = FactLineage.tryMergeAll(toCheck)
    val interesting = toCheck.exists(_.lineage.lastKey.isAfter(IOService.STANDARD_TIME_FRAME_END))
    (res.isDefined,interesting)
  }

  def gather() = {
    connectedComponentFiles.foreach(f => {
      val g = FactMergeabilityGraph.loadComponent(f,subdomain)
      g.edges.foreach(e => {
        val tr1 = e.tupleReferenceA.toTupleReference(byAssociationID(e.tupleReferenceA.associationID))
        val tr2 = e.tupleReferenceB.toTupleReference(byAssociationID(e.tupleReferenceB.associationID))
        val evidenceCount = tr1.getDataTuple.head.getOverlapEvidenceCount(tr2.getDataTuple.head)
        val prev = evidenceCountHistogram.getOrElse(evidenceCount,0)
        evidenceCountHistogram(evidenceCount) = prev+1
        val (isValid,isInteresting) = getValidityAndInterestingness(tr1,tr2)
        if(isValid){
          val prev = validEvidenceCountHistogram.getOrElse(evidenceCount,0)
          validEvidenceCountHistogram(evidenceCount) = prev+1
        }
        if(isValid && isInteresting){
          val prev = validAndInterestingEvidenceCountHistogram.getOrElse(evidenceCount,0)
          validAndInterestingEvidenceCountHistogram(evidenceCount) = prev+1
        }
      })
    })
    logger.debug("Histogram of edges by evidence count")
    AggregatedHistogram(evidenceCountHistogram).printAll()
    logger.debug(s"total edge count: ${evidenceCountHistogram.values.sum}")
    logger.debug(s"total edge count with evidence >0: ${evidenceCountHistogram.filter(_._1>0).values.sum}")
    logger.debug("Histogram of all valid edges by evidence count")
    AggregatedHistogram(validEvidenceCountHistogram).printAll()
    logger.debug(s"total edge count: ${validEvidenceCountHistogram.values.sum}")
    logger.debug(s"total edge count with evidence >0: ${validEvidenceCountHistogram.filter(_._1>0).values.sum}")
    logger.debug("Histogram of all valid and interesting edges by evidence count")
    AggregatedHistogram(validAndInterestingEvidenceCountHistogram).printAll()
    logger.debug(s"total edge count: ${validAndInterestingEvidenceCountHistogram.values.sum}")
    logger.debug(s"total edge count with evidence >0: ${validAndInterestingEvidenceCountHistogram.filter(_._1>0).values.sum}")
  }
}
