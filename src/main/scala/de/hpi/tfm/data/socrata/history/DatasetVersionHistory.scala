package de.hpi.tfm.data.socrata.history

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.socrata.{DatasetInstance, JsonReadable, JsonWritable}
import de.hpi.tfm.io.IOService

import java.time.LocalDate
import scala.collection.mutable
import scala.io.Source

class DatasetVersionHistory(val id:String,
                            val versionsWithChanges:mutable.ArrayBuffer[LocalDate] = mutable.ArrayBuffer[LocalDate](),
                            var deletions:mutable.ArrayBuffer[LocalDate] = mutable.ArrayBuffer[LocalDate]()) extends JsonWritable[DatasetVersionHistory]{
  def latestChangeTimestamp: LocalDate = versionsWithChanges.sortBy(_.toEpochDay).last

  def allVersionsIncludingDeletes = (versionsWithChanges.toSet ++ deletions.toSet)
    .toIndexedSeq
    .sortBy(_.toEpochDay)


  def makeVersionsDeletes(versionsToIgnore: mutable.HashSet[LocalDate]) = {
    versionsToIgnore.toSeq.sortBy(_.toEpochDay).foreach(v => {
      val index = versionsWithChanges.indexOf(v)
      val removedVersion = versionsWithChanges.remove(index)
      val i = deletions.lastIndexWhere(d => d.toEpochDay<removedVersion.toEpochDay)+1
      deletions.insert(i,removedVersion)
    })
    //clean up deletes
    if(versionsWithChanges.isEmpty)
      deletions.clear()
    else
      deletions = deletions.dropWhile(d => d.toEpochDay<versionsWithChanges.head.toEpochDay)
  }


  def removeIgnoredVersions(toIgnore: Iterable[DatasetInstance]) = {
    toIgnore.foreach{case DatasetInstance(_,versionToIgnore) => {
      val index = versionsWithChanges.indexOf(versionToIgnore)
      val removedVersion = versionsWithChanges.remove(index)
      if(!deletions.isEmpty){
        assert(removedVersion!=versionsWithChanges.head)
        val versionBeforeRemoved = versionsWithChanges(index-1)
        val deletionBetween = deletions.filter(v => v.isAfter(versionBeforeRemoved) && v.isBefore(removedVersion))
        assert(deletionBetween.size<=1)
        if(!deletionBetween.isEmpty)
          deletions.remove(deletions.indexOf(deletionBetween.head))
      }
    }}
  }


}

object DatasetVersionHistory extends JsonReadable[DatasetVersionHistory] with StrictLogging{
  def load() = fromJsonObjectPerLineFile(IOService.getCleanedVersionHistoryFile().getAbsolutePath)

  def loadAsMap() = load()
    .map(h => (h.id,h))
    .toMap
  /***
   * Removes a version and a previous delete from the history for each version
   */
  def removeVersionIgnore(histories: Iterable[DatasetVersionHistory]) = {
    val versionIgnoreFile = IOService.getVersionIgnoreFile()
    val versionIgnores = Source.fromFile(versionIgnoreFile)
      .getLines()
      .toSeq
      .tail
      .toSet
      .map((s:String) => {
        val tokens = s.split(",")
        DatasetInstance(tokens(0),LocalDate.parse(tokens(1),IOService.dateTimeFormatter))
      })
      .groupBy(_.id)
    histories.foreach(h => {
      val toIgnore = versionIgnores.getOrElse(h.id,Set())
      h.removeIgnoredVersions(toIgnore)
    })
  }
}
