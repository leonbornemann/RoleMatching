package de.hpi.dataset_versioning.db_synthesis.evaluation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.ChangeCube
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.optimization.{GreedyEdgeWeightOptimizer, TupleMerge}
import de.hpi.dataset_versioning.db_synthesis.preparation.simplifiedExport.FactLookupTable
import de.hpi.dataset_versioning.db_synthesis.sketches.field.{AbstractTemporalField, TemporalFieldTrait}
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
  val statFile = new PrintWriter("stats.csv")
  statFile.println("isValid,numNonEqualAtT,numEqualAtT,numOverlappingTransitions,MI,entropyReduction")
  mergesAsTupleReferences.foreach{case (tm,clique) => {
    val toCheck = clique.map(vertex => {
      val surrogateKey = vertex.table.getRow(vertex.rowIndex).keys.head
      //TODO: we need to look up that surrogate key in the bcnf reference table
      val vl = factLookupTables(vertex.toIDBasedTupleReference.associationID).getCorrespondingValueLineage(surrogateKey)
      vl
    }).toIndexedSeq
    val isValid = evalResult.checkValidityAndUpdateCount(toCheck)
    if(toCheck.size==2){
      val vl1 = toCheck(0).keepOnlyStandardTimeRange
      val vl2 = toCheck(1).keepOnlyStandardTimeRange
      var numEqual = 0
      var numUnEqual = 0
      for(date <- IOService.STANDARD_TIME_RANGE){
        val valueA = vl1.valueAt(date)
        val valueB = vl1.valueAt(date)
        if(!ValueLineage.isWildcard(valueA) && !ValueLineage.isWildcard(valueB) || valueA==valueB){
          assert(valueA == valueB)
          numEqual+=1
        } else{
          numUnEqual+=1
        }
      }
      //statFile.println("isValid,numNonEqualAtT,numEqualAtT,numOverlappingTransitions,MI,entropyReduction")
      val entropyReduction = AbstractTemporalField.ENTROPY_REDUCTION_SET_FIELD(Set(vl1, vl2))
      val mutualInformation = vl1.mutualInformation(vl2)
      val evidence = vl1.getOverlapEvidenceCount(vl2)
      statFile.println(s"$isValid,$numUnEqual,$numEqual,$evidence,$mutualInformation,$entropyReduction")
    }
    if(isValid) validMerges += tm else invalidMerges +=tm
    }
    logger.debug("Finished processing ")
  }
  statFile.close()
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
