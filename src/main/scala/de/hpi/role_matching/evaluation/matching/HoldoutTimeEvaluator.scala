package de.hpi.role_matching.evaluation.matching

import de.hpi.data_preparation.socrata.tfmp_input.association.AssociationIdentifier
import de.hpi.data_preparation.socrata.tfmp_input.factLookup.FactLookupTable
import de.hpi.data_preparation.socrata.tfmp_input.table.nonSketch.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.role_matching.cbrm.compatibility_graph.GraphConfig
import de.hpi.role_matching.cbrm.compatibility_graph.role_tree.RoleReference

import java.time.LocalDate

abstract class HoldoutTimeEvaluator(trainGraphConfig:GraphConfig,evaluationGraphConfig:GraphConfig) {

  val factLookupTables = scala.collection.mutable.HashMap[AssociationIdentifier, FactLookupTable]()
  val byAssociationID = scala.collection.mutable.HashMap[AssociationIdentifier,  SurrogateBasedSynthesizedTemporalDatabaseTableAssociation]()

  def getFactLookupTable(id: AssociationIdentifier) = {
    factLookupTables.getOrElseUpdate(id,FactLookupTable.readFromStandardFile(id))
  }

  def getAssociation(associationID: AssociationIdentifier) = {
    byAssociationID.getOrElseUpdate(associationID,SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(associationID))
  }

  def getValidityAndInterestingness(references: IndexedSeq[RoleReference[Any]]): (Boolean,Boolean) = {
    val originalAndtoCheck = references
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
    val interesting = originals.exists(hasRealChangeAfterTrainPeriod(_))
    (res.isDefined,interesting)
  }

  //Real change Definition: Either Wildcard to nonWildcard or non-wildcard to different non-wildcard! TODO: decide whcih variant is best suited
//  def getPointInTimeOfRealChangeAfterTrainPeriod(lineage: FactLineage) = {
//    val prev = if(lineage.lineage.contains(trainGraphConfig.timeRangeEnd)) lineage.lineage(trainGraphConfig.timeRangeEnd) else lineage.lineage.maxBefore(trainGraphConfig.timeRangeEnd).get
//    val it = lineage.lineage.iteratorFrom(trainGraphConfig.timeRangeEnd)
//    var pointInTime:Option[LocalDate] = None
//    while(it.hasNext && !pointInTime.isDefined){
//      val (curTIme,curValue) = it.next()
//      if(lineage.isWildcard(prev) && !lineage.isWildcard(curValue) || !lineage.isWildcard(prev) && !lineage.isWildcard(curValue) && curValue!=prev){
//        pointInTime = Some(curTIme)
//      }
//    }
//    pointInTime
//  }

  //Real change Definition: Non-Wildcard to new Non-Wildcard
  def getPointInTimeOfRealChangeAfterTrainPeriod(lineage: FactLineage) = {
    val prevNonWcValue = lineage.lineage.filter(t => !lineage.isWildcard(t._2) && !t._1.isAfter(trainGraphConfig.timeRangeEnd)).lastOption
    if(prevNonWcValue.isEmpty)
      None
    else {
      val it = lineage.lineage.iteratorFrom(trainGraphConfig.timeRangeEnd)
      var pointInTime:Option[LocalDate] = None
      while(it.hasNext && !pointInTime.isDefined){
        val (curTIme,curValue) = it.next()
        if(!lineage.isWildcard(curValue) && curValue!=prevNonWcValue.get._2){
          pointInTime = Some(curTIme)
        }
      }
      pointInTime
    }
  }

  def referencesToOriginal(references:IndexedSeq[RoleReference[Any]]) = {
    val originals = references
      .map(vertex => {
        val surrogateKey = vertex.table.getRow(vertex.rowIndex).keys.head
        //TODO: we need to look up that surrogate key in the bcnf reference table
        getFactLookupTable(vertex.toIDBasedTupleReference.associationID).getCorrespondingValueLineage(surrogateKey)
      })
    originals
  }

  def getEarliestPointInTimeOfRealChangeAfterTrainPeriod(lineages: IndexedSeq[RoleReference[Any]]) = {
    val candidates  = referencesToOriginal(lineages)
      .map(getPointInTimeOfRealChangeAfterTrainPeriod(_))
      .filter(_.isDefined)
      .map(_.get)
    if(candidates.isEmpty) None
    else Some(candidates.minBy(_.toEpochDay))
  }

  def hasRealChangeAfterTrainPeriod(lineage: FactLineage): Boolean = {
    getPointInTimeOfRealChangeAfterTrainPeriod(lineage).isDefined
  }

  def getNumVerticesWithChangeAfterTrainPeriod(references: IndexedSeq[RoleReference[Any]]) = {
    referencesToOriginal(references).filter(hasRealChangeAfterTrainPeriod(_)).size
  }

}
