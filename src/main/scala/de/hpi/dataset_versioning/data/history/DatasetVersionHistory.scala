package de.hpi.dataset_versioning.data.history

import java.io.{File, PrintWriter}
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data
import de.hpi.dataset_versioning.data.parser.JsonDataParser
import de.hpi.dataset_versioning.data.{DatasetInstance, JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.io.IOService

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


  def removeIgnoredVersionsBasedOnMissingCustomMetadata(list: collection.Seq[DatasetVersionHistory],reportFile:PrintWriter) = {
    val md = IOService.getOrLoadCustomMetadataForStandardTimeFrame()
    val lineagesToRemove = mutable.HashSet[DatasetVersionHistory]()
    var count = 0
    list.foreach(lineage =>{
      if(count % 1000 == 0){
        logger.debug(s"processed $count out of ${list.size} lineages")
      }
      val versionsToIgnore = mutable.HashSet[LocalDate]()
      for (i <- 0 until lineage.versionsWithChanges.size) {
        val curVersion = lineage.versionsWithChanges(i)
        val dsInstance = data.DatasetInstance(lineage.id, curVersion)
        if(!md.metadata.contains(dsInstance)){
          //try parsing the dataset
          val ds = IOService.tryLoadDataset(dsInstance,true)
          if(ds.erroneous || ds.rows.isEmpty){
            versionsToIgnore.add(curVersion)
          } else{
            logger.debug("Weird - found no custom metadata, but file is parseable.")
          }
        }
      }
      val originalSize = lineage.versionsWithChanges.size
      lineage.makeVersionsDeletes(versionsToIgnore)
      if(lineage.versionsWithChanges.isEmpty)
        lineagesToRemove +=lineage
      count +=1
      reportFile.println(s"${lineage.id},$originalSize,${versionsToIgnore.size}")
    })
    lineagesToRemove.foreach(l => {
      logger.debug(s"Removing ${l.id} ")
    })
    logger.debug(s"Removing ${lineagesToRemove.size} lineages entirely")
    list.filter(lineage => !lineagesToRemove.contains(lineage))
  }


  def removeVersionIgnoreBasedOnHashEquality(list: collection.Seq[DatasetVersionHistory],ignoreFile:File) = {
    val md = IOService.getOrLoadCustomMetadataForStandardTimeFrame
    val versionsToIgnore = mutable.HashMap[String,mutable.HashSet[DatasetInstance]]()
    list.foreach(lineage => {
      for (i <- 1 until lineage.versionsWithChanges.size) {
        val prevVersion = lineage.versionsWithChanges(i-1)
        val curIgnoreCandidateVersion = lineage.versionsWithChanges(i)
        if(!md.metadata.contains(data.DatasetInstance(lineage.id,prevVersion))){
          println(lineage.id)
        } else {
          val prevMD = md.metadata(data.DatasetInstance(lineage.id, prevVersion))
          if(md.metadata.contains(data.DatasetInstance(lineage.id, curIgnoreCandidateVersion))){
            val curMD = md.metadata(data.DatasetInstance(lineage.id, curIgnoreCandidateVersion))
            if (prevMD.tupleSpecificHash == curMD.tupleSpecificHash) {
              val toIgnore = versionsToIgnore.getOrElseUpdate(lineage.id, mutable.HashSet[DatasetInstance]())
              toIgnore.add(data.DatasetInstance(lineage.id, curIgnoreCandidateVersion))
            }
          }
        }
      }
      /*println("---------------------------------------------------------------------------------------------")
      println(lineage.versionsWithChanges)
      println(lineage.deletions)
      println(versionsToIgnore(lineage.id))*/
      lineage.removeIgnoredVersions(versionsToIgnore.getOrElseUpdate(lineage.id,mutable.HashSet[DatasetInstance]()))
    })
    val pr = new PrintWriter(ignoreFile)
    versionsToIgnore.foreach{case (s,ignored) => pr.println(s"$s,${ignored.size}")}
    pr.close()
  }


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
        data.DatasetInstance(tokens(0),LocalDate.parse(tokens(1),IOService.dateTimeFormatter))
      })
      .groupBy(_.id)
    histories.foreach(h => {
      val toIgnore = versionIgnores.getOrElse(h.id,Set())
      h.removeIgnoredVersions(toIgnore)
    })
  }
}
