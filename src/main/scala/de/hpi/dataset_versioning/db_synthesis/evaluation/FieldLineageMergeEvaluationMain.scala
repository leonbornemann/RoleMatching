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
      if(res.isDefined) numCorrect +=1 else numIncorrect +=1
    }}
    logger.debug(s"Found $numCorrect and $numIncorrect in this file (accuracy: ${numCorrect / (numIncorrect+numCorrect).toDouble})")
    totalNumCorrect += numCorrect
    totalNumIncorrect += numIncorrect
  }
  logger.debug(s"Found $totalNumCorrect and $totalNumIncorrect in total (accuracy: ${totalNumCorrect / (totalNumIncorrect+totalNumCorrect).toDouble})")

}
