package de.hpi.tfm.data.wikipedia.infobox.transformed

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.wikipedia.infobox.original.{InfoboxRevision, InfoboxRevisionHistory}
import de.hpi.tfm.data.wikipedia.infobox.statistics.vertex.WikipediaInfoboxStatistiicsGatherer

import java.io.{File, PrintWriter}
import java.time.{Duration, Period}

object WikipediaInfoboxValueHistoryCreationMain extends App with StrictLogging {
  val file = args(0)
  val resultDir = new File(args(1))
  val granularityInDays = args(2).toInt
  val statGatherer = new WikipediaInfoboxStatistiicsGatherer(new File(args(3)))
  InfoboxRevisionHistory.setGranularityInDays(granularityInDays)
  val objects = InfoboxRevision.fromJsonObjectPerLineFile(file)
  objects.foreach(_.checkIntegrity())
  val revisionHistories = InfoboxRevisionHistory.getFromRevisionCollection(objects)
  revisionHistories.foreach(rh => rh.integrityCheck())
  logger.debug(s"Found ${revisionHistories.size} infobox histories to process")
  var finished = 0
  val resultFile = resultDir.getAbsolutePath + "/" + WikipediaInfoboxValueHistory.getFilenameForBucket(new File(file).getName)
  val pr = new PrintWriter(resultFile)
  var filtered = 0
  var total = 0
  revisionHistories
    .foreach(r => {
      val res = r.toWikipediaInfoboxValueHistories
      val weird = res.filter(_.lineage.lineage.keySet.exists(_.isAfter(InfoboxRevisionHistory.LATEST_HISTORY_TIMESTAMP)))
      assert(weird.size==0)
      val retained = res.filter(vh => {
        val statLine = vh.toWikipediaInfoboxStatisticsLine
        val nonWildcardPeriod = vh.lineage.toFactLineage.nonWildcardDuration(InfoboxRevisionHistory.LATEST_HISTORY_TIMESTAMP)
        val hasRealChange = statLine.totalRealChanges >= 1
        val hasEnoughNonWildcard = nonWildcardPeriod.getDays >= Period.ofYears(1).getDays
        hasRealChange && hasEnoughNonWildcard //very basic filtering to weed out uninteresting infoboxes / property lineages
      })
      filtered += (res.size - retained.size)
      total += res.size
      retained.foreach(_.appendToWriter(pr,false,true))
      statGatherer.addToFile(retained)
      finished += 1
      if (finished % 100 == 0) {
        logger.debug(s"Finished $finished infobox histories leading to ${total} num facts of which we discarded ${filtered} (${100*filtered / total.toDouble}%)")
      }
    })
  pr.close()
  //try reading:
  val res = WikipediaInfoboxValueHistory.fromJsonObjectPerLineFile(resultFile)
  statGatherer.closeFile()
}
