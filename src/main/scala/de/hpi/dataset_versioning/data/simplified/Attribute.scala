package de.hpi.dataset_versioning.data.simplified

case class Attribute(name:String,id:Int,var position:Option[Int] = None,var humanReadableName:Option[String]=None) {

}
