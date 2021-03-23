package de.hpi.dataset_versioning.db_synthesis.evaluation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.ChangeCube
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.optimization.{GreedyEdgeWeightOptimizer, TupleMerge}
import de.hpi.dataset_versioning.db_synthesis.preparation.simplifiedExport.FactLookupTable
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait
import de.hpi.dataset_versioning.io.IOService

import java.io.{File, PrintWriter}

object FieldLineageMergeEvaluationMain extends App with StrictLogging{
  IOService.socrataDir = args(0)
  private val methodName = GreedyEdgeWeightOptimizer.methodName
  val files:Seq[File] =
    if(args.length>1)
      Seq(TupleMerge.getStandardJsonObjectPerLineFile(args(1),methodName))
    else
      TupleMerge.getStandardObjectPerLineFiles(methodName)
  val evalResult = TupleMergeEvaluationResult()
  val merges = files
    .flatMap(f => {
      logger.debug(s"Reading merge file $f")
      TupleMerge.fromJsonObjectPerLineFile(f.getAbsolutePath)
        .filter(_.clique.size>1)
    })
  logger.debug("Loaded merges")
  val tables = merges
    .flatMap(_.clique.map(_.associationID).toSet)
    .toSet
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
  val validMerges = collection.mutable.ArrayBuffer[TupleMerge]()
  val invalidMerges = collection.mutable.ArrayBuffer[TupleMerge]()
  mergesAsTupleReferences.foreach{case (tm,clique) => {
    val toCheck = clique.map(vertex => {
      val surrogateKey = vertex.table.getRow(vertex.rowIndex).keys.head
      //TODO: we need to look up that surrogate key in the bcnf reference table
      val vl = factLookupTables(vertex.toIDBasedTupleReference.associationID).getCorrespondingValueLineage(surrogateKey)
      vl
    }).toIndexedSeq
    val isValid = evalResult.checkValidityAndUpdateCount(toCheck)
    if(isValid) validMerges += tm else invalidMerges +=tm
    }
    logger.debug("Finished processing ")
  }
  logger.debug(s"Found final result $evalResult")
  evalResult.printStats()
  evalResult.writeToStandardFile(methodName)
  val prCorrect = new PrintWriter(TupleMerge.getCorrectMergeFile(methodName))
  validMerges.foreach(m => m.appendToWriter(prCorrect,false,true))
  prCorrect.close()
  val prIncorrect = new PrintWriter(TupleMerge.getIncorrectMergeFile(methodName))
  invalidMerges.foreach(m => m.appendToWriter(prIncorrect,false, true))
  prIncorrect.close()
}
