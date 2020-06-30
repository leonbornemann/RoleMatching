package de.hpi.dataset_versioning.data.simplified

case class Attribute(var name:String,var id:Int,var position:Option[Int] = None,var humanReadableName:Option[String]=None) {

}
