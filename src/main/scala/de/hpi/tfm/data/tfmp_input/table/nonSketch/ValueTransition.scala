package de.hpi.tfm.data.tfmp_input.table.nonSketch

case class ValueTransition[A](prev: A, after: A) {

  def nullSafeToString(any: A) = if (any == null) "null" else any.toString

  def toShortString = (nullSafeToString(prev) + " -> " + nullSafeToString(after).toString).replace("\n", " ")

}
