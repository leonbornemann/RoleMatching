package de.hpi.tfm.data.wikipedia.infobox

import java.io.{File, PrintWriter}

class WikipediaInfoboxStatistiicsGatherer(file:File) {
  def closeFile() = pr.close()

  val pr = new PrintWriter(file)

  pr.println(WikipediaInfoboxStatisticsLine.getSchema.mkString(","))

  def addLineToFile(vh: WikipediaInfoboxValueHistory) = {
    pr.println(vh.toWikipediaInfoboxStatisticsLine.getCSVLine)
  }

  def addToFile(vhs: collection.Seq[WikipediaInfoboxValueHistory]) = {
    vhs.foreach(vh => addLineToFile(vh))
  }

}
