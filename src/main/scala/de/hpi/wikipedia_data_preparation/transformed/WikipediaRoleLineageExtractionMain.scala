package de.hpi.wikipedia_data_preparation.transformed

import com.typesafe.scalalogging.StrictLogging
import de.hpi.wikipedia_data_preparation.original_infobox_data.{InfoboxRevision, InfoboxRevisionHistory, WikipediaLineageCreationMode}

import java.io.{File, PrintWriter}
import java.time.LocalDate

object WikipediaRoleLineageExtractionMain extends App with StrictLogging {
  val file = args(0)
  val resultDir = new File(args(1))
  val granularityInDays = args(2).toInt
  val mode = WikipediaLineageCreationMode.withName(args(3))
  val minDecayProbability = if(mode == WikipediaLineageCreationMode.PROBABILISTIC_DECAY_FUNCTION) Some(args(4).toDouble) else None
  val trainTimeEnd = if(mode == WikipediaLineageCreationMode.PROBABILISTIC_DECAY_FUNCTION) Some(LocalDate.parse(args(5))) else None
  InfoboxRevisionHistory.setGranularityInDays(granularityInDays)
  InfoboxRevisionHistory.lineageCreationMode = Some(mode)
  InfoboxRevisionHistory.minDecayProbability = minDecayProbability
  InfoboxRevisionHistory.trainTimeEnd = trainTimeEnd
  val objects = InfoboxRevision.fromJsonObjectPerLineFile(file)
  objects.foreach(_.checkIntegrity())
  val revisionHistories = InfoboxRevisionHistory.getFromRevisionCollection(objects)
  revisionHistories.foreach(rh => rh.integrityCheck())
  logger.debug(s"Running with mode $mode and probability : $minDecayProbability")
  logger.debug(s"Running granularity (days) $granularityInDays")
  logger.debug(s"Found ${revisionHistories.size} infobox histories to process")
  var finished = 0
  val resultFile = resultDir.getAbsolutePath + "/" + WikipediaRoleLineage.getFilenameForBucket(new File(file).getName)
  val pr = new PrintWriter(resultFile)
  var filtered = 0
  var total = 0
  revisionHistories
    .foreach(r => {
      val res = r.toWikipediaInfoboxValueHistories
      val weird = res.filter(_.lineage.lineage.keySet.exists(_.isAfter(InfoboxRevisionHistory.LATEST_HISTORY_TIMESTAMP)))
      assert(weird.size==0)
      val retained = res.filter(vh => {
        vh.isOfInterest
         //very basic filtering to weed out uninteresting infoboxes / property lineages
      })
      filtered += (res.size - retained.size)
      total += res.size
      retained.foreach(_.appendToWriter(pr,false,true))
      finished += 1
      if (finished % 100 == 0) {
        logger.debug(s"Finished $finished infobox histories leading to ${total} num facts of which we discarded ${filtered} (${100*filtered / total.toDouble}%)")
      }
    })
  pr.close()
}
