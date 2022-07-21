package de.hpi.role_matching.data

object Util {
  def toDittoSaveString(str: String) = toCSVSafe(str).replace('\t','_').replace(' ','_')

  def nullSafeToString(value: Any): String = if(value==null) "null" else value.toString

  def toCSVSafe(id: String) = id.replace('\r',' ').replace('\n',' ').replace(',',' ')

}
