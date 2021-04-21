package de.hpi.tfm.data.wikipedia.infobox.query

import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.data.wikipedia.infobox.transformed.WikipediaInfoboxValueHistory

case class WikipediaInfoboxValueHistoryMatch(a: WikipediaInfoboxValueHistory, b: WikipediaInfoboxValueHistory) extends JsonWritable[WikipediaInfoboxValueHistory]{



}

object WikipediaInfoboxValueHistoryMatch extends JsonReadable[WikipediaInfoboxValueHistory]{

}
