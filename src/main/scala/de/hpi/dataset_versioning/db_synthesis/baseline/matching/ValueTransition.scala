package de.hpi.dataset_versioning.db_synthesis.baseline.matching

case class ValueTransition[A](prev: A, after: A) {

  def nullSafeToString(any:A) = if(any==null) "null" else any.toString

  def toShortString = (nullSafeToString(prev) + " -> " +nullSafeToString(after).toString).replace("\n"," ")

}
