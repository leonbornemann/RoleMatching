package de.hpi.wikipedia_data_preparation.original_infobox_data

object WikipediaLineageCreationMode extends Enumeration {
  type WikipediaLineageCreationMode = Value
  val WILDCARD_BETWEEN_ALL_CONFIRMATIONS,WILDCARD_BETWEEN_CHANGE,WILDCARD_OUTSIDE_OF_GRACE_PERIOD,PROBABILISTIC_DECAY_FUNCTION,NO_DECAY = Value
}
