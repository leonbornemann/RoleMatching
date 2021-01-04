package de.hpi.dataset_versioning.db_synthesis.baseline.decomposition

import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.TemporalSchema
import de.hpi.dataset_versioning.db_synthesis.database.table.AssociationSchema
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

class DecompositionCompleter(subdomain:String) {

  def completeDecomposition(id:String) = {
    if(!DBSynthesis_IOService.associationSchemataExist(subdomain, id)){
      println(s"found missing for $id")
    } else{
      val schemaHistory = TemporalSchema.load(id).attributes
      val numAssociations = AssociationSchema.loadAllAssociations(subdomain, id).size
      if(schemaHistory.size!=numAssociations){
        println(s"incomplete: $id")
      }
    }
  }

}
