package de.hpi.tfm.data.wikipedia.infobox.query

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.data.wikipedia.infobox.transformed.WikipediaInfoboxValueHistory

case class WikipediaInfoboxValueHistoryMatch(a: WikipediaInfoboxValueHistory, b: WikipediaInfoboxValueHistory) extends JsonWritable[WikipediaInfoboxValueHistoryMatch]{
  def toWikipediaEdgeStatRow(graphConfig: GraphConfig, timestampResolutionInDays: Int) =
    WikipediaEdgeStatRow(this,timestampResolutionInDays,graphConfig)


}

object WikipediaInfoboxValueHistoryMatch extends JsonReadable[WikipediaInfoboxValueHistoryMatch]{

}
