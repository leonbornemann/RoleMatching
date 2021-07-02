package de.hpi.socrata.diff.semantic

import de.hpi.socrata.DatasetInstance
import de.hpi.socrata.io.Socrata_IOService

import java.time.LocalDate

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
    val previousDatasets = Socrata_IOService.extractDataToWorkingDir(previous)
      .map(f => new DatasetInstance(Socrata_IOService.filenameToID(f), previous))
      .toSet
    val currentDatasets = Socrata_IOService.extractDataToWorkingDir(current)
      .map(f => new DatasetInstance(Socrata_IOService.filenameToID(f), current))
      .toSet
    val groundTruth = new GroundTruthDatasetMatching()
      .matchDatasets(previousDatasets, currentDatasets)
    groundTruth
  }
}
