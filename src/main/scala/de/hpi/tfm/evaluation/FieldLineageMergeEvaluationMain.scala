package de.hpi.tfm.evaluation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.tfmp_input.factLookup.FactLookupTable
import de.hpi.tfm.data.tfmp_input.table.{AbstractTemporalField, TemporalFieldTrait}
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{FactLineage, SurrogateBasedSynthesizedTemporalDatabaseTableAssociation}
import de.hpi.tfm.fact_merging.optimization.{GreedyEdgeWeightOptimizer, TupleMerge}
import de.hpi.tfm.io.IOService

import java.io.{File, PrintWriter}

object FieldLineageMergeEvaluationMain extends App with StrictLogging{
  IOService.socrataDir = args(0)
  private val methodName = GreedyEdgeWeightOptimizer.methodName
  private val subdomain = args(1)
  val files:Seq[File] =
    if(args.length>1)
      Seq(TupleMerge.getStandardJsonObjectPerLineFile(subdomain,methodName,args(2)))
    else
      TupleMerge.getStandardObjectPerLineFiles(subdomain,methodName)
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
  logger.debug(s"Input Fact count: ${factLookupTables.map(_._2.surrogateKeyToVL.size).sum}")
  val byAssociationID = tables
    .map(id => (id,SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(id)))
    .toMap
  logger.debug("Loaded associations")
  val mergesAsTupleReferences = merges
    .map(tm => (tm,tm.clique.map(idbtr => idbtr.toTupleReference(byAssociationID(idbtr.associationID)))))
  val validMerges = collection.mutable.ArrayBuffer[TupleMerge]()
  val invalidMerges = collection.mutable.ArrayBuffer[TupleMerge]()
  val statFile = new PrintWriter("stats.csv")
  statFile.println("isValid,numNonEqualAtT,numEqualAtT,numOverlappingTransitions,MI,entropyReduction,newScore")
  var statFileEntries = 0
  mergesAsTupleReferences.foreach{case (tm,clique) => {
    val toCheck = clique
      .toIndexedSeq
      .map(vertex => {
      val surrogateKey = vertex.table.getRow(vertex.rowIndex).keys.head
      //TODO: we need to look up that surrogate key in the bcnf reference table
      val vl = factLookupTables(vertex.toIDBasedTupleReference.associationID).getCorrespondingValueLineage(surrogateKey)
      vl
    })
    val isValid = evalResult.checkValidityAndUpdateCount(toCheck)
    if(toCheck.size==2){
      val vl1 = toCheck(0).keepOnlyStandardTimeRange
      val vl2 = toCheck(1).keepOnlyStandardTimeRange
      var numEqual = 0
      var numUnEqual = 0
      for(date <- IOService.STANDARD_TIME_RANGE){
        val valueA = vl1.valueAt(date)
        val valueB = vl1.valueAt(date)
        if(!FactLineage.isWildcard(valueA) && !FactLineage.isWildcard(valueB)){
          assert(valueA == valueB)
          numEqual+=1
        } else{
          numUnEqual+=1
        }
      }
      statFileEntries+=1
      val entropyReduction = AbstractTemporalField.ENTROPY_REDUCTION_SET_FIELD(Set[TemporalFieldTrait[Any]](vl1, vl2))
      val mutualInformation = vl1.mutualInformation(vl2)
      val evidence = vl1.getOverlapEvidenceCount(vl2)
      val newScore = vl1.newScore(vl2)
      statFile.println(s"$isValid,$numUnEqual,$numEqual,$evidence,$mutualInformation,$entropyReduction,$newScore")
    }
    if(isValid) validMerges += tm else invalidMerges +=tm
    }
    logger.debug("Finished processing ")
  }
  statFile.close()
  logger.debug(s"Found final result $evalResult")
  evalResult.printStats()
  evalResult.writeToStandardFile(subdomain,methodName)
  if(evalResult.total!=merges.size){
    println(s"WHat? ${evalResult.total} and ${merges.size}")
  }
  if(merges.filter(_.clique.size==2).size!=statFileEntries){
    println(s"??? ${merges.filter(_.clique.size==2).size} and $statFileEntries")
  }
  val prCorrect = new PrintWriter(TupleMerge.getCorrectMergeFile(subdomain, methodName))
  validMerges.foreach(m => m.appendToWriter(prCorrect,false,true))
  prCorrect.close()
  val prIncorrect = new PrintWriter(TupleMerge.getIncorrectMergeFile(subdomain,methodName))
  invalidMerges.foreach(m => m.appendToWriter(prIncorrect,false, true))
  prIncorrect.close()
}
