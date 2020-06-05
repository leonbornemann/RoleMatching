package de.hpi.dataset_versioning.data.diff.semantic

import java.time.LocalDate

import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.io.IOService

class GroundTruthDatasetMatching {

  val matching = new DatasetMatching()
  def isDatasetFile(str: String): Boolean = str.endsWith(".json")

  def matchDatasets(previous: Set[DatasetInstance], current: Set[DatasetInstance]) = {
    val currentIds = current.map(d => (d.id,d))
        .toMap
    previous.foreach( d => {
      if(currentIds.contains(d.id)){
        matching.matchings.put(d,currentIds(d.id))
      } else{
        matching.deletes.add(d)
      }
    })
    currentIds.keySet.diff(previous.map(_.id)).foreach(d => matching.inserts.add(currentIds(d)))
    matching
  }
}
object GroundTruthDatasetMatching {
  def getGroundTruthMatching(previous: LocalDate, current: LocalDate) = {
    val previousDatasets = IOService.extractDataToWorkingDir(previous)
      .map(f => new DatasetInstance(IOService.filenameToID(f), previous))
      .toSet
    val currentDatasets = IOService.extractDataToWorkingDir(current)
      .map(f => new DatasetInstance(IOService.filenameToID(f), current))
      .toSet
    val groundTruth = new GroundTruthDatasetMatching()
      .matchDatasets(previousDatasets, currentDatasets)
    groundTruth
  }
}
