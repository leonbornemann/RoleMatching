package de.hpi.wikipedia.statistics

import de.hpi.wikipedia.data.transformed.WikipediaInfoboxValueHistory

import java.io.{File, PrintWriter}

class WikipediaInfoboxStatistiicsGatherer(file:File) {

  def closeFile() = pr.close()

  val pr = new PrintWriter(file)

  pr.println(WikipediaInfoboxStatisticsLine.getSchema.mkString(","))

  def addLineToFile(vh: WikipediaInfoboxValueHistory) = {
    pr.println(vh.toWikipediaInfoboxStatisticsLine.getCSVLine)
  }

  def addToFile(vhs: collection.Iterable[WikipediaInfoboxValueHistory]) = {
    vhs.foreach(vh => addLineToFile(vh))
  }

}
