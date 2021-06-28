package de.hpi.tfm.util

object LogUtil {

  def buildLogProgressStrings(count: Int,step:Int, max: Option[Int], thing: String): Option[String] = {
    if(count % step==0){
      if(max.isDefined)
        Some(s"Processed $count out of ${max.get} $thing (${100*count / max.get.toDouble}%)")
      else
        Some(s"Processed $count $thing")
    } else {
      None
    }

  }


}
