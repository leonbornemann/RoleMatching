package de.hpi.dataset_versioning.data.metadata

case class Classification(categories:Array[String], //an array of categories that have been algorithmically assigned
                         tags:Array[String], //	an array of tags that have been algorithmically assigned
                         domain_category:Option[String], // the singular category given to the asset by the owning domain
                         domain_tags:Array[String], //an array of tags given to the asset by the owning domain
                         domain_metadata:Array[Any], //an array of domain metadata objects for public custom metadata
                         domain_private_metadata:Array[Any] //if you are authenticated to view it, an array of domain metadata objects for private custom metadata
                         ) {

}
