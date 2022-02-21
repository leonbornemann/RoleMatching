package de.hpi.role_matching.cbrm.data

object Util {

  def nullSafeToString(value: Any): String = if(value==null) "null" else value.toString

  def toCSVSafe(id: String) = id.replace('\r',' ').replace('\n',' ').replace(',',' ')

}
