package de.hpi.wikipedia_data_preparation.transformed

import com.typesafe.scalalogging.StrictLogging
import de.hpi.wikipedia_data_preparation.original_infobox_data.{InfoboxRevision, InfoboxRevisionHistory, WikipediaLineageCreationMode}

import java.io.{File, PrintWriter}
import java.time.LocalDate

object WikipediaRoleLineageExtractionMain extends App with StrictLogging {
  val file = args(0)
  val resultRootDir = new File(args(1))
  val granularitiesInDays = args(2).split(",").map(_.toInt)
  val modes = args(3).split(",").map(WikipediaLineageCreationMode.withName(_))
  val minDecayProbabilities = args(4).split(",").map(_.toDouble)
  val trainTimeEnds = args(5).split(",").map(LocalDate.parse(_))
  val objects = InfoboxRevision.fromJsonObjectPerLineFile(file)
  objects.foreach(_.checkIntegrity())
  val revisionHistories = InfoboxRevisionHistory.getFromRevisionCollection(objects)
  revisionHistories.foreach(rh => rh.integrityCheck())
  logger.debug(s"Found ${revisionHistories.size} infobox histories to process")

  def runForResultDir(resultDir: File,statRootDir:File,trainTimeEnd:LocalDate) = {
    resultDir.mkdirs()
    var finished = 0
    val resultFile = resultDir.getAbsolutePath + "/" + WikipediaRoleLineage.getFilenameForBucket(new File(file).getName)
    val resultFileStats = new File(statRootDir.getAbsolutePath + "/" + WikipediaRoleLineage.getFilenameForBucket(new File(file).getName))
    val pr = new PrintWriter(resultFile)
    var filtered = 0
    var total = 0
    val templateStats = TemplateStats(collection.mutable.HashMap())
    revisionHistories
      .foreach(r => {
        val res = r.toWikipediaInfoboxValueHistories.toIndexedSeq
        val weird = res.filter(_.lineage.lineage.keySet.exists(_.isAfter(InfoboxRevisionHistory.LATEST_HISTORY_TIMESTAMP)))
        assert(weird.size==0)
        val retained = res.filter(vh => {
          vh.isOfInterest(trainTimeEnd)
          //very basic filtering to weed out uninteresting infoboxes / property lineages
        }).toIndexedSeq
        filtered += (res.size - retained.size)
        total += res.size
        retained.foreach(_.appendToWriter(pr,false,true))
        templateStats.addAll(retained)
        finished += 1
        if (finished % 100 == 0) {
          logger.debug(s"Finished $finished infobox histories leading to ${total} num facts of which we discarded ${filtered} (${100*filtered / total.toDouble}%)")
        }
      })
    pr.close()
    logger.debug(s"Gathered stats about ${templateStats.nameToCount.size} templates")
    templateStats.toJsonFile(resultFileStats)
  }

  for(granularityInDays <- granularitiesInDays; mode <- modes; trainTimeEnd <- trainTimeEnds){
    InfoboxRevisionHistory.setGranularityInDays(granularityInDays)
    InfoboxRevisionHistory.lineageCreationMode = Some(mode)
    InfoboxRevisionHistory.trainTimeEnd = Some(trainTimeEnd)
    if(mode==WikipediaLineageCreationMode.PROBABILISTIC_DECAY_FUNCTION){
      for( minDecayProbability <- minDecayProbabilities){
        InfoboxRevisionHistory.minDecayProbability = Some(minDecayProbability)
        val curResultDir = new File(resultRootDir.getAbsolutePath + s"/${mode.toString}_${minDecayProbability}_${granularityInDays}_$trainTimeEnd/wikipediaLineages/")
        val statRootDir = new File(resultRootDir.getAbsolutePath + s"/${mode.toString}_${minDecayProbability}_${granularityInDays}_$trainTimeEnd/templateStats/")
        statRootDir.mkdirs()
        runForResultDir(curResultDir,statRootDir,trainTimeEnd)
      }
    } else {
      val curResultDir = new File(resultRootDir.getAbsolutePath + s"/${mode.toString}_${granularityInDays}_$trainTimeEnd/wikipediaLineages/")
      val curStatDir = new File(resultRootDir.getAbsolutePath + s"/${mode.toString}_${granularityInDays}_$trainTimeEnd/stats/")
      curStatDir.mkdirs()
      runForResultDir(curResultDir,curStatDir,trainTimeEnd)
    }
  }


}
