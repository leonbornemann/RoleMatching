package de.hpi.dataset_versioning.data.simplified

@SerialVersionUID(3L)
case class Attribute(var name:String,var id:Int,var position:Option[Int] = None,var humanReadableName:Option[String]=None) extends Serializable{

}
