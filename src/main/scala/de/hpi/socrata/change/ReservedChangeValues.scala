package de.hpi.socrata.change

object ReservedChangeValues {
  def NE_DISPLAY = "⊥"

  val NOT_EXISTANT_ROW = "\u200c⊥R\u200c"
  val NOT_EXISTANT_COL = "\u200c⊥C\u200c"
  val NOT_EXISTANT_DATASET = "\u200c⊥D\u200c"
  val NOT_EXISTANT_CELL = "\u200c⊥CE\u200c"
  val NOT_KNOWN_DUE_TO_NO_VISIBLE_CHANGE = "\u200c⊥V\u200c"

}
