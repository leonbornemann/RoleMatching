package de.hpi.dataset_versioning.db_synthesis.baseline.matching

case class ValueTransition(prev: Any, after: Any) {

  def nullSafeToString(any:Any) = if(any==null) "null" else any.toString

  def toShortString = (nullSafeToString(prev) + " -> " +nullSafeToString(after).toString).replace("\n"," ")

}
