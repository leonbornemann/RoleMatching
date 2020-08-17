package de.hpi.dataset_versioning.db_synthesis.baseline.decomposition

case class DecomposedTemporalTableIdentifier(subdomain:String,viewID:String,bcnfID:Int,associationID:Option[Int]) {
  def compositeID: String = subdomain + "." + viewID + "." + bcnfID + (if(associationID.isDefined) "_" + associationID.get.toString else "")

}

object DecomposedTemporalTableIdentifier {
  def fromFilename(fileName: String) = {
    val tokens1 = fileName.split("\\.")
    val subdomain = tokens1(0)
    val viewID = tokens1(1)
    if(tokens1(2).contains("_")){
      val tokens2 = tokens1(2).split("_")
      val bcnfID = tokens2(0).toInt
      val associationID = Some(tokens2(1).toInt)
      DecomposedTemporalTableIdentifier(subdomain,viewID,bcnfID,associationID)
    } else{
      val bcnfID = tokens1(3).toInt
      DecomposedTemporalTableIdentifier(subdomain,viewID,bcnfID,None)
    }
  }
}
