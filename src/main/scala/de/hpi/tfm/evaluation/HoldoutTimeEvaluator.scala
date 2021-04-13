package de.hpi.tfm.evaluation

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.tfmp_input.factLookup.FactLookupTable
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{FactLineage, SurrogateBasedSynthesizedTemporalDatabaseTableAssociation}

import java.time.LocalDate
import scala.Console.in

abstract class HoldoutTimeEvaluator(trainGraphConfig:GraphConfig,evaluationGraphConfig:GraphConfig) {

  val factLookupTables = scala.collection.mutable.HashMap[AssociationIdentifier, FactLookupTable]()
  val byAssociationID = scala.collection.mutable.HashMap[AssociationIdentifier,  SurrogateBasedSynthesizedTemporalDatabaseTableAssociation]()

  def getFactLookupTable(id: AssociationIdentifier) = {
    factLookupTables.getOrElseUpdate(id,FactLookupTable.readFromStandardFile(id))
  }

  def getAssociation(associationID: AssociationIdentifier) = {
    byAssociationID.getOrElseUpdate(associationID,SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(associationID))
  }

  def getValidityAndInterestingness(references: IndexedSeq[TupleReference[Any]]): (Boolean,Boolean) = {
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

  def getPointInTimeOfRealChangeAfterTrainPeriod(lineage: FactLineage) = {
    val prev = if(lineage.lineage.contains(trainGraphConfig.timeRangeEnd)) lineage.lineage(trainGraphConfig.timeRangeEnd) else lineage.lineage.maxBefore(trainGraphConfig.timeRangeEnd).get
    val it = lineage.lineage.iteratorFrom(trainGraphConfig.timeRangeEnd)
    var pointInTime:Option[LocalDate] = None
    while(it.hasNext && !pointInTime.isDefined){
      val (curTIme,curValue) = it.next()
      if(lineage.isWildcard(prev) && !lineage.isWildcard(curValue) || !lineage.isWildcard(prev) && !lineage.isWildcard(curValue) && curValue!=prev){
        pointInTime = Some(curTIme)
      }
    }
    pointInTime
  }

  def referencesToOriginal(references:IndexedSeq[TupleReference[Any]]) = {
    val originals = references
      .map(vertex => {
        val surrogateKey = vertex.table.getRow(vertex.rowIndex).keys.head
        //TODO: we need to look up that surrogate key in the bcnf reference table
        getFactLookupTable(vertex.toIDBasedTupleReference.associationID).getCorrespondingValueLineage(surrogateKey)
      })
    originals
  }

  def getEarliestPointInTimeOfRealChangeAfterTrainPeriod(lineages: IndexedSeq[TupleReference[Any]]) = {
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

  def getNumVerticesWithChangeAfterTrainPeriod(references: IndexedSeq[TupleReference[Any]]) = {
    referencesToOriginal(references).filter(hasRealChangeAfterTrainPeriod(_)).size
  }

}
