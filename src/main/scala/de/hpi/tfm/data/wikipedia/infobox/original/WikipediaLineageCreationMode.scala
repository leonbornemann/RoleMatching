package de.hpi.tfm.data.wikipedia.infobox.original

object WikipediaLineageCreationMode extends Enumeration {
  type WikipediaLineageCreationMode = Value
  val WILDCARD_BETWEEN_ALL_CONFIRMATIONS,WILDCARD_BETWEEN_CHANGE,WILDCARD_OUTSIDE_OF_GRACE_PERIOD = Value
}
