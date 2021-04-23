package de.hpi.tfm.util

object CSVUtil {

  def toCleanString(value: Any) = value.toString.replace(",",";").replace('\r','_').replace('\n','_')

}
