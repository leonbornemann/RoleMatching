package de.hpi.tfm.data.socrata.change

object ReservedChangeValues {
  def NE_DISPLAY = "⊥"

  val NOT_EXISTANT_ROW = "\u200c⊥R\u200c"
  val NOT_EXISTANT_COL = "\u200c⊥C\u200c"
  val NOT_EXISTANT_DATASET = "\u200c⊥D\u200c"
  val NOT_EXISTANT_CELL = "\u200c⊥CE\u200c"
  val NOT_KNOWN_DUE_TO_TIMESTAMP_RESOLUTION = "\u200c⊥T\u200c"

}
