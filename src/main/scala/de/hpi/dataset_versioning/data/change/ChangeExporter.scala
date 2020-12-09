package de.hpi.dataset_versioning.data.change

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.data.simplified.RelationalDataset
import de.hpi.dataset_versioning.io.IOService

import java.io.File
import java.time.LocalDate
import scala.collection.mutable

class ChangeExporter extends StrictLogging{

  var histories = DatasetVersionHistory.fromJsonObjectPerLineFile(IOService.getCleanedVersionHistoryFile().getAbsolutePath)
    .map(h => (h.id, h))
    .toMap

  def updateLastValues(lastValues: mutable.HashMap[(Long, Int), Any], cube: ChangeCube) = {
    cube.allChanges.foreach(c => lastValues((c.e,c.pID)) = c.value)
  }

  def updateColumnSetsAtTime(columnSetsAtTimestamp: mutable.TreeMap[LocalDate, Set[Int]], ds:RelationalDataset) = {
    columnSetsAtTimestamp(ds.version) = ds.attributes.map(_.id).toSet
  }

  def exportAllChangesFromVersions(id: String, allVersions: IndexedSeq[LocalDate]) = {
    //we probably need to track all change records and their last values
    var curDs:RelationalDataset = null
    val firstVersion = allVersions(0)
    val finalChangeCube = ChangeCube(id)
    val lastValues = scala.collection.mutable.HashMap[(Long,Int),Any]()
    val enteredInitialValues = scala.collection.mutable.HashMap[(Long,Int),Boolean]()
    val columnSetsAtTimestamp = scala.collection.mutable.TreeMap[LocalDate,Set[Int]]()
    if(!versionExists(id,firstVersion))
      logger.debug(s"Skipping $id because no first version was found")
    else {
      var prevDs = IOService.loadSimplifiedRelationalDataset(DatasetInstance(id, firstVersion))
      val changeFile = IOService.getChangeFile(id)
      val cube = getChanges(RelationalDataset.createEmpty(id, LocalDate.MIN), prevDs)
      updateLastValues(lastValues,cube)
      updateColumnSetsAtTime(columnSetsAtTimestamp,prevDs)
      addNewChanges(finalChangeCube,cube,enteredInitialValues,columnSetsAtTimestamp)
      finalChangeCube.addToAttributeNameMapping(prevDs.version,prevDs.attributes)
      for (i <- 1 until allVersions.size) {
        val curVersion = allVersions(i)
        if (versionExists(id, curVersion)) {
          curDs = IOService.loadSimplifiedRelationalDataset(DatasetInstance(id, curVersion))
        } else {
          curDs = RelationalDataset.createEmpty(id, curVersion) //we have a delete
        }
        val curChanges = getChanges(prevDs, curDs)
        //before we update last values we have to determine the new inserts:
        val newlyInsertedEntities = curChanges.allChanges.map(_.e).toSet.diff(lastValues.keySet.map(_._1))
        updateLastValues(lastValues,curChanges)
        updateColumnSetsAtTime(columnSetsAtTimestamp,curDs)
        if(curDs.isEmpty && prevDs.isEmpty){
          //nothing to do
        } else if(curDs.isEmpty && !prevDs.isEmpty){
          //we add dataset delete for all in lastValues!
          val allTracked = lastValues.keySet.toIndexedSeq
          allTracked.foreach{case (e,p) => {
            val v = lastValues((e,p))
            assert(v != ReservedChangeValues.NOT_EXISTANT_DATASET)
            finalChangeCube.addChange(Change(curDs.version,e,p,ReservedChangeValues.NOT_EXISTANT_DATASET))
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
          //for all existing entities we need to check if they have no entry for any of the currently present columns,if so we need to set them to NOT_EXISTANT_ROW for that column
          //TODO: this is currently very slow, but as it is mostly a one-shot process, we don't really care
//          val existingEntities = lastValues.keySet.map(_._1)
//          existingEntities.foreach(e => {
//            for(p<- newDsAttrSet){
//              if(!lastValues.contains((e,p))){
//                val newVal = ReservedChangeValues.NOT_EXISTANT_ROW
//                curChanges.addChange(Change(curDs.version,e,p,newVal))
//                //update last values:
//                lastValues((e,p))=newVal
//              }
//            }
//          })
          //for all newly inserted entities - we need to update old columnvalues
//          val oldCols = lastValues.keySet.map(_._2).diff(newDsAttrSet)
//          oldCols.foreach(p => {
//            for(e <- newlyInsertedEntities){
//              if(lastValues.contains((e,p))){
//                assert(lastValues((2,p))==ReservedChangeValues.NOT_EXISTANT_COL)
//              } else {
//                val newVal = ReservedChangeValues.NOT_EXISTANT_COL
//                curChanges.addChange(Change(curDs.version, e, p, newVal))
//                //update last values:
//                lastValues((e, p)) = newVal
//              }
//            }
//          })
        }
        finalChangeCube.addToAttributeNameMapping(curDs.version,curDs.attributes)
        addNewChanges(finalChangeCube,curChanges,enteredInitialValues,columnSetsAtTimestamp)
        prevDs = curDs
      }
      var byEntityProperty = finalChangeCube.allChanges.groupBy(c => (c.e,c.pID))
      val allEntities = finalChangeCube.allChanges.map(_.e).toSet
      val allProperties = finalChangeCube.allChanges.map(_.pID).toSet
      if(curDs!= null && curDs.version!=IOService.STANDARD_TIME_FRAME_START){
        for(e <- allEntities){
          for (p <- allProperties){
            val key = (e,p)
            if(!byEntityProperty.contains(key)){
              //we have to add this:
              assert(!enteredInitialValues.contains(key))
              assert(!lastValues.contains(key))
              val colSetsToConsider = columnSetsAtTimestamp
              addInitialHistoryToChangeCube(e,p,columnSetsAtTimestamp,colSetsToConsider,finalChangeCube)
            }
          }
        }
      }
      byEntityProperty = finalChangeCube.allChanges.groupBy(c => (c.e,c.pID))
      byEntityProperty.foreach{case (k,v) => {
          val sortedByTime = v.sortBy(_.t.toEpochDay)
          assert(sortedByTime.head.t==IOService.STANDARD_TIME_FRAME_START)
          val vals = sortedByTime.map(_.value)
          for(i <- 1 until vals.size){
            if(vals(i)==vals(i-1))
              println()
            assert(vals(i)!=vals(i-1))
          }
        }}
      finalChangeCube.toJsonFile(new File(changeFile))
    }
  }

  private def addNewChanges(finalChangeCube: ChangeCube,
                            newCube: ChangeCube,
                            enteredInitialValues:scala.collection.mutable.HashMap[(Long,Int),Boolean],
                            columnSetsAtTimestamp:scala.collection.mutable.TreeMap[LocalDate,Set[Int]]) = {
    newCube.allChanges.foreach { case Change(t, e, p, v) => {
      if (enteredInitialValues.getOrElse((e, p), false) == false && t != IOService.STANDARD_TIME_FRAME_START) {
        //we need to add intitial values for all timestamps before this one
        val colSetsToConsider = columnSetsAtTimestamp
          .filter{ case (ts, _) => ts.isBefore(t) }
        addInitialHistoryToChangeCube(e,p,columnSetsAtTimestamp,colSetsToConsider,finalChangeCube)
      }
      enteredInitialValues((e,p)) = true
      finalChangeCube.addChange(Change(t, e, p, v))
    }
    }
  }

  def addInitialHistoryToChangeCube(e: Long, p: Int, columnSetsAtTimestamp: mutable.TreeMap[LocalDate, Set[Int]], colSetsToConsider: mutable.TreeMap[LocalDate, Set[Int]], finalChangeCube: ChangeCube) = {
    var lastVal = ReservedChangeValues.NOT_EXISTANT_DATASET
    if (columnSetsAtTimestamp.firstKey != IOService.STANDARD_TIME_FRAME_START) {
      //add non-existant dataset until the first timestamp
      finalChangeCube.addChange(Change(IOService.STANDARD_TIME_FRAME_START, e, p, ReservedChangeValues.NOT_EXISTANT_DATASET))
    } else {
      lastVal = null
    }
    colSetsToConsider.foreach { case (ts, colSet) => {
      if (colSet.isEmpty) {
        //dataset delete!
        assert(lastVal != ReservedChangeValues.NOT_EXISTANT_DATASET)
        finalChangeCube.addChange(Change(ts, e, p, ReservedChangeValues.NOT_EXISTANT_DATASET))
        lastVal = ReservedChangeValues.NOT_EXISTANT_DATASET
      } else if (!colSet.contains(p)) {
        if (lastVal == ReservedChangeValues.NOT_EXISTANT_COL) {
          //no change to add
        } else {
          finalChangeCube.addChange(Change(ts, e, p, ReservedChangeValues.NOT_EXISTANT_COL))
          lastVal = ReservedChangeValues.NOT_EXISTANT_COL
        }
      } else {
        if (lastVal == ReservedChangeValues.NOT_EXISTANT_ROW) {
          //no change to add
        } else {
          finalChangeCube.addChange(Change(ts, e, p, ReservedChangeValues.NOT_EXISTANT_ROW))
          lastVal = ReservedChangeValues.NOT_EXISTANT_ROW
        }
      }
    }
    }
  }

  def exportAllChanges(id: String) = {
    val allVersions = histories(id).allVersionsIncludingDeletes
    //handle first insert separately:
    exportAllChangesFromVersions(id,allVersions)
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
