package de.hpi.role_matching.data

case class ValueTransition(prev: Any, after: Any) {

  def nullSafeToString(any: Any) = if (any == null) "null" else any.toString

  def toShortString = (nullSafeToString(prev) + " -> " + nullSafeToString(after).toString).replace("\n", " ")

}
