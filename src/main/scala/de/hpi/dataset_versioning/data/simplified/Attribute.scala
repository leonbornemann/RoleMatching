package de.hpi.dataset_versioning.data.simplified

case class Attribute(var name:String,var id:Int,var position:Option[Int] = None,var humanReadableName:Option[String]=None) extends Serializable{
  private def serialVersionUID = 6529685098267757670L


}
