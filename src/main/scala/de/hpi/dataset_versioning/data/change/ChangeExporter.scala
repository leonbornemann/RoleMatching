package de.hpi.dataset_versioning.data.change

import java.io.{File, PrintWriter}
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.data.simplified.RelationalDataset
import de.hpi.dataset_versioning.io.IOService

class ChangeExporter extends StrictLogging{

  var histories = DatasetVersionHistory.fromJsonObjectPerLineFile(IOService.getCleanedVersionHistoryFile().getAbsolutePath)
    .map(h => (h.id, h))
    .toMap

  def exportAllChanges(id: String,ignoreInitialInsert:Boolean=false) = {
    val allVersions = histories(id).allVersionsIncludingDeletes
    //handle first insert separately:
    var nextDs:RelationalDataset = null
    val firstVersion = allVersions(0)
    val changeCube = ChangeCube(id)
    if(!versionExists(id,firstVersion))
      logger.debug(s"Skipping $id because no first version was found")
    else {
      var curDs = IOService.loadSimplifiedRelationalDataset(DatasetInstance(id, firstVersion))
      val changeFile = IOService.getChangeFile(id)
      if (!ignoreInitialInsert) {
        changeCube.addAll(getChanges(RelationalDataset.createEmpty(id, LocalDate.MIN), curDs))
      }
      for (i <- 1 until allVersions.size - 1) {
        val curVersion = allVersions(i)
        if (versionExists(id, curVersion))
          nextDs = IOService.loadSimplifiedRelationalDataset(DatasetInstance(id, curVersion))
        else
          nextDs = RelationalDataset.createEmpty(id, curVersion) //we have a delete
        val curChanges = getChanges(curDs, nextDs)
        changeCube.addAll(curChanges)
        curDs = nextDs
      }
      changeCube.toJsonFile(new File(changeFile))
    }
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
