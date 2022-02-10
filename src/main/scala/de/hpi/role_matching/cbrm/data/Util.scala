package de.hpi.role_matching.cbrm.data

object Util {
  def toCSVSafe(id: String) = id.replace('\r',' ').replace('\n',' ').replace(',',' ')

}
