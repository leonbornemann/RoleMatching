package de.hpi.dataset_versioning.data.metadata.custom

import java.time.LocalDate

import de.hpi.dataset_versioning.data.JsonWritable

case class CustomMetadata(id:String,
                          var link:Option[String]=None,
                          intID:Int, //unique for all versions of all datasets
                          version:LocalDate,
                          nrows:Int,
                          schemaSpecificHash:Int,
                          tupleSpecificHash:Int,
                          columnMetadata: Map[String,ColumnCustomMetadata], //maps colname to its metadata
                         ) extends JsonWritable[CustomMetadata]{

  def topLevelDomain = {
    val domains = link.getOrElse("").split("https://")(1).split("/")(0).split("\\.").reverse.toSeq
    val topLvlDomains = if (domains.size>=2) domains.slice(0,2).mkString(".") else domains(0)
    topLvlDomains
  }
  def ncols = columnMetadata.size
  val columnMetadataByID = columnMetadata.map{case(_,cm) => (cm.shortID,cm)}

}
