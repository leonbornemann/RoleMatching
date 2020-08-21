package de.hpi.dataset_versioning.data.change

import java.io.{File, PrintWriter}
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.data.simplified.RelationalDataset
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

class ChangeExporter extends StrictLogging{

  var histories = DatasetVersionHistory.fromJsonObjectPerLineFile(IOService.getCleanedVersionHistoryFile().getAbsolutePath)
    .map(h => (h.id, h))
    .toMap

  def updateLastValues(lastValues: mutable.HashMap[(Long, Int), Any], cube: ChangeCube) = {
    cube.allChanges.foreach(c => lastValues((c.e,c.pID)) = c.value)
  }

  def exportAllChangesFromVersions(id: String, allVersions: IndexedSeq[LocalDate], ignoreInitialInsert: Boolean) = {
    //we probably need to track all change records and their last values
    var curDs:RelationalDataset = null
    val firstVersion = allVersions(0)
    val changeCube = ChangeCube(id)
    val lastValues = scala.collection.mutable.HashMap[(Long,Int),Any]()
    if(!versionExists(id,firstVersion))
      logger.debug(s"Skipping $id because no first version was found")
    else {
      var prevDs = IOService.loadSimplifiedRelationalDataset(DatasetInstance(id, firstVersion))
      val changeFile = IOService.getChangeFile(id)
      if (!ignoreInitialInsert) {
        val cube = getChanges(RelationalDataset.createEmpty(id, LocalDate.MIN), prevDs)
        updateLastValues(lastValues,cube)
        changeCube.addAll(cube)
      }
      for (i <- 1 until allVersions.size) {
        val curVersion = allVersions(i)
        if (versionExists(id, curVersion)) {
          curDs = IOService.loadSimplifiedRelationalDataset(DatasetInstance(id, curVersion))
        } else {
          curDs = RelationalDataset.createEmpty(id, curVersion) //we have a delete
        }
        val curChanges = getChanges(prevDs, curDs)
        updateLastValues(lastValues,curChanges)
        if(curDs.isEmpty && prevDs.isEmpty){
          //nothing to do
        } else if(curDs.isEmpty && !prevDs.isEmpty){
          //we add dataset delete for all in lastValues!
          val allTracked = lastValues.keySet.toIndexedSeq
          allTracked.foreach{case (e,p) => {
            val v = lastValues((e,p))
            if(v==ReservedChangeValues.NOT_EXISTANT_DATASET){
              println()
            }
            assert(v != ReservedChangeValues.NOT_EXISTANT_DATASET)
            changeCube.addChange(Change(curDs.version,e,p,ReservedChangeValues.NOT_EXISTANT_DATASET))
            lastValues((e,p)) = ReservedChangeValues.NOT_EXISTANT_DATASET
          }}
        } else if(prevDs.isEmpty){
          assert(versionExists(id,curVersion))
          //we need to update this for all changes, even those that are not in curChanges
          val newDsAttrSet = curDs.attributes.map(_.id).toSet
          val toUpdate = lastValues
            .filter(_._2==ReservedChangeValues.NOT_EXISTANT_DATASET)
          toUpdate.foreach{ case((e,p),_) => {
              val newVal = if(newDsAttrSet.contains(p)) ReservedChangeValues.NOT_EXISTANT_ROW else ReservedChangeValues.NOT_EXISTANT_COL
              curChanges.addChange(Change(curDs.version,e,p,newVal))
              //update last values:
              lastValues((e,p))=newVal
            }}
        } else {
          val newDsAttrSet = curDs.attributes.map(_.id).toSet
          //we need to check if all NOT_EXISTANT_ROW values are still NOT_EXISTANT_ROW or now NOT_EXISTANT_COL
          var toUpdate = lastValues
            .filter{ case ((e,p),v) => v == ReservedChangeValues.NOT_EXISTANT_ROW && !newDsAttrSet.contains(p)}
          toUpdate.foreach{ case((e,p),_) => {
            val newVal = ReservedChangeValues.NOT_EXISTANT_COL
            curChanges.addChange(Change(curDs.version,e,p,newVal))
            //update last values:
            lastValues((e,p))=newVal
          }}
          //we need to check if all NOT_EXISTANT_COL values are still NOT_EXISTANT_COL or now NOT_EXISTANT_ROW:
          toUpdate = lastValues
            .filter{ case ((e,p),v) => v == ReservedChangeValues.NOT_EXISTANT_COL && newDsAttrSet.contains(p)}
          toUpdate.foreach{ case((e,p),_) => {
            val newVal = ReservedChangeValues.NOT_EXISTANT_ROW
            curChanges.addChange(Change(curDs.version,e,p,newVal))
            //update last values:
            lastValues((e,p))=newVal
          }}
        }
        changeCube.addAll(curChanges)
        prevDs = curDs
      }
      changeCube.allChanges.groupBy(c => (c.e,c.pID))
        .foreach{case (k,v) => {
          val vals = v.sortBy(_.t.toEpochDay).map(_.value)
          for(i <- 1 until vals.size){
            assert(vals(i)!=vals(i-1))
          }
        }}
      changeCube.toJsonFile(new File(changeFile))
    }
  }

  def exportAllChanges(id: String, ignoreInitialInsert:Boolean=false) = {
    val allVersions = histories(id).allVersionsIncludingDeletes
    //handle first insert separately:
    exportAllChangesFromVersions(id,allVersions,ignoreInitialInsert)
  }

  private def getChanges(prevDs: RelationalDataset, nextDs: RelationalDataset) = {
    val changes = DiffAsChangeCube.fromDatasetVersions(prevDs, nextDs) //TODO: change this back
      .changeCube
    changes
  }

  private def versionExists(id: String, curVersion: LocalDate) = {
    new File(IOService.getSimplifiedDatasetFile(DatasetInstance(id, curVersion))).exists()
  }
}
