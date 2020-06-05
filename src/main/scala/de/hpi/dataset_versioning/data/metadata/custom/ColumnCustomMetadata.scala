package de.hpi.dataset_versioning.data.metadata.custom

import de.hpi.dataset_versioning.data.metadata.custom.ColumnDatatype.ColumnDatatype

case class ColumnCustomMetadata(id:String,shortID:Short,hash:Int,uniqueness:Double,dataType:ColumnDatatype) {

}
