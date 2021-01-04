package de.hpi.dataset_versioning.db_synthesis.baseline.decomposition

import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.AttributeLineage
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.TemporalSchema
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.natural_key_based.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.database.table.AssociationSchema
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

class DecompositionCompleter(subdomain:String) {

  val histories = DatasetVersionHistory.loadAsMap()

  def createNewBCNF(viewID: String, attributes: collection.IndexedSeq[AttributeLineage],newBCNFID:Int) = {
    val dttID = DecomposedTemporalTableIdentifier(subdomain,viewID,newBCNFID,None)
    val schema = TemporalSchema.load(viewID)
    val dtt = new DecomposedTemporalTable(dttID,
      scala.collection.mutable.ArrayBuffer() ++ attributes,
      attributes.toSet,
      IOService.STANDARD_TIME_RANGE.map(v => {
        (v,attributes.map(a => a.valueAt(v))
          .filter(_._2.exists)
          .map(_._2.attr.get)
          .toSet)
      }).toMap,
      scala.collection.mutable.HashSet()
    )
    //dtt.writeToStandardFile()
    val versionHistory = histories(viewID)
    val a = new TemporalTableDecomposer(subdomain,viewID,versionHistory)
    a.createSurrogateBasedDtts(IndexedSeq(dtt))
  }

  def completeDecomposition(id:String) = {
    if(!DBSynthesis_IOService.associationSchemataExist(subdomain, id)){
      createNewBCNF(id,TemporalSchema.load(id).attributes,0)
      println(s"found missing BCNF for $id, creating a single new one")
    } else{
      val schemaHistory = TemporalSchema.load(id).attributes
      val associationSchemata = AssociationSchema.loadAllAssociations(subdomain, id)
      if(schemaHistory.size!=associationSchemata.size){
        println(s"incomplete: $id, beginning to complete")
        val coveredLineages = associationSchemata.map(_.attributeLineage)
        val uncoveredLineages = schemaHistory.diff(coveredLineages)
        val newBcnfID = associationSchemata.map(_.id.bcnfID).max +1
        createNewBCNF(id,uncoveredLineages,newBcnfID)
      }
    }
  }

}
