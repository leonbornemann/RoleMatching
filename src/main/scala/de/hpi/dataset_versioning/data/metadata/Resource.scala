package de.hpi.dataset_versioning.data.metadata

case class Resource(name:String, id:String,
                    description:Option[String],
                    attribution:Option[String],
                    asset_type:Option[String], //TODO: we need to alias this in deserialization and serialization!
                    updatedAt:String, //TODO: make this and the following 3 values timestamps/localdatetimes
                    createdAt:String,
                    metadata_updated_at:String,
                    data_updated_at:String,
                    page_views:Any,
                    columns_field_name:Array[String],
                    columns_name:Array[String],
                    columns_description:Array[String],
                    columns_dataytpe:Array[String],
                    columns_format:Array[Any], //TODO: maybe make this more precise
                    parent_fxf:Array[String], //contains ids of other datasets
                    provenance:Provenance.Value,
                    download_count:Option[Long]
                   ) {

}
