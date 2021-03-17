package de.hpi.dataset_versioning.db_synthesis.evaluation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.ChangeCube
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.optimization.TupleMerge
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait
import de.hpi.dataset_versioning.io.IOService

object FieldLineageMergeEvaluationMain extends App with StrictLogging{
  IOService.socrataDir = args(0)
  val files = TupleMerge.getStandardObjectPerLineFiles
  var totalNumCorrect = 0
  var totalNumIncorrect = 0
  var totalNumCorrectIntersting = 0
  var totalNumIncorrectInteresting = 0
  for(file <- files){
    val merges = TupleMerge.fromJsonObjectPerLineFile(file.getAbsolutePath)
    val tables = merges.flatMap(_.clique.map(_.associationID).toSet).toSet
    val byAssociationID = tables
      .map(id => (id,SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(id)))
      .toMap
    val mergesAsTupleReferences = merges
      .map(tm => (tm,tm.clique.map(idbtr => idbtr.toTupleReference(byAssociationID(idbtr.associationID)))))
    val viewIDs = byAssociationID.keySet.map(_.viewID)
    val cube = new FieldLineageFromChangeCubes(ChangeCube.loadAllChanges(viewIDs.toIndexedSeq))
    var numCorrect = 0
    var numIncorrect = 0
    var numInterestingAndCorrect = 0
    var numInterestingAndInCorrect = 0
    mergesAsTupleReferences.foreach{case (tm,clique) => {
      val toCheck = clique.map(vertex => {
        val tableID = vertex.toIDBasedTupleReference.associationID.viewID
        val attrID = vertex.table.getNonKeyAttribute.attrId
        val entityID = vertex.rowIndex
        cube.getFieldLineage(tableID,attrID,entityID)
      }).toIndexedSeq
      //do a simple does it still work check?
      var res = Option(toCheck.head)
      (1 until toCheck.size).foreach(i => {
        if(res.isDefined)
          res = res.get.tryMergeWithConsistent(toCheck(i))
      })
      val interesting = toCheck.exists(_.lineage.lastKey.isAfter(IOService.STANDARD_TIME_FRAME_END))
      if(res.isDefined) numCorrect +=1 else numIncorrect +=1
      if(res.isDefined && interesting) numInterestingAndCorrect += 1
      if(res.isDefined && !interesting) numInterestingAndInCorrect += 1
    }}
    logger.debug(s"Found $numCorrect correct and $numIncorrect incorrect merges in this file (accuracy: ${numCorrect / (numIncorrect+numCorrect).toDouble})")
    logger.debug(s"Found ${numInterestingAndCorrect + numInterestingAndInCorrect} merges that are interesting to evaluate (${(numInterestingAndCorrect+numInterestingAndInCorrect) / (numCorrect + numIncorrect).toDouble}))")
    logger.debug(s"Found $numInterestingAndCorrect correct and interesting and $numInterestingAndInCorrect incorrect and interesting in this file (accuracy: ${numInterestingAndCorrect / (numInterestingAndInCorrect+numInterestingAndCorrect).toDouble})")
    totalNumCorrect += numCorrect
    totalNumIncorrect += numIncorrect
    totalNumCorrectIntersting += numInterestingAndCorrect
    totalNumIncorrectInteresting += numInterestingAndInCorrect
  }
  logger.debug(s"Found $totalNumCorrect correct and $totalNumIncorrect incorrect merges in total (accuracy: ${totalNumCorrect / (totalNumIncorrect+totalNumCorrect).toDouble})")
  logger.debug(s"Found $totalNumCorrectIntersting correct and interesting and $totalNumIncorrectInteresting incorrect and interesting in total (accuracy: ${totalNumCorrectIntersting / (totalNumCorrectIntersting+totalNumIncorrectInteresting).toDouble})")
}
