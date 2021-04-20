package de.hpi.tfm.data.wikipedia.infobox

import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}

case class WikipediaInfoboxValueHistoryMatch(a: WikipediaInfoboxValueHistory, b: WikipediaInfoboxValueHistory) extends JsonWritable[WikipediaInfoboxValueHistory]{



}

object WikipediaInfoboxValueHistoryMatch extends JsonReadable[WikipediaInfoboxValueHistory]{

}
