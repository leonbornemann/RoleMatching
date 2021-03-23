package de.hpi.dataset_versioning.db_synthesis.evaluation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.ChangeCube
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.optimization.{GreedyEdgeWeightOptimizer, TupleMerge}
import de.hpi.dataset_versioning.db_synthesis.preparation.simplifiedExport.FactLookupTable
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait
import de.hpi.dataset_versioning.io.IOService

import java.io.File

object FieldLineageMergeEvaluationMain extends App with StrictLogging{
  IOService.socrataDir = args(0)
  private val methodName = GreedyEdgeWeightOptimizer.methodName
  val files:Seq[File] =
    if(args.length>1)
      Seq(TupleMerge.getStandardJsonObjectPerLineFile(args(1),methodName))
    else
      TupleMerge.getStandardObjectPerLineFiles(methodName)
  val evalResul = TupleMergeEvaluationResult()
  val merges = files.flatMap(f => TupleMerge.fromJsonObjectPerLineFile(f.getAbsolutePath))
  logger.debug("Loaded merges")
  val tables = merges.flatMap(_.clique.map(_.associationID).toSet).toSet
  val factLookupTables = tables
    .map(id => (id,FactLookupTable.readFromStandardFile(id)))
    .toMap
  logger.debug("Loaded fact lookup tables")
  val byAssociationID = tables
    .map(id => (id,SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(id)))
    .toMap
  logger.debug("Loaded associations")
  val mergesAsTupleReferences = merges
    .map(tm => (tm,tm.clique.map(idbtr => idbtr.toTupleReference(byAssociationID(idbtr.associationID)))))
  mergesAsTupleReferences.foreach{case (tm,clique) => {
    val toCheck = clique.map(vertex => {
      val surrogateKey = vertex.table.getRow(vertex.rowIndex).keys.head
      //TODO: we need to look up that surrogate key in the bcnf reference table
      val vl = factLookupTables(vertex.toIDBasedTupleReference.associationID).getCorrespondingValueLineage(surrogateKey)
      vl
    }).toIndexedSeq
    //do a simple does it still work check?
    var res = Option(toCheck.head)
    (1 until toCheck.size).foreach(i => {
      if(res.isDefined)
        res = res.get.tryMergeWithConsistent(toCheck(i))
    })
    evalResul.updateCount(res)
    }
    logger.debug("Finished processing ")
  }
  logger.debug(s"Found final result $evalResul")
  evalResul.writeToStandardFile(methodName)
}
