package de.hpi.dataset_versioning.db_synthesis.database.table

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.SurrogateAttributeLineage
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.{AttributeLineageWithHashMap, TemporalSchema}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.surrogate_based.SurrogateBasedDecomposedTemporalTableHelper
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

import scala.collection.mutable.ArrayBuffer

@SerialVersionUID(3L)
case class BCNFTableSchema(val id: DecomposedTemporalTableIdentifier,
                           val surrogateKey: IndexedSeq[SurrogateAttributeLineage],
                           val attributes: scala.collection.mutable.IndexedSeq[SurrogateAttributeLineage],
                           val foreignSurrogateKeysToReferencedBCNFTables: IndexedSeq[(SurrogateAttributeLineage, collection.IndexedSeq[DecomposedTemporalTableIdentifier])]) extends JsonWritable[BCNFTableSchema] {

  override def toString: String = id + s"(${surrogateKey.mkString(",")}  ,${attributes.mkString(",")})"

  assert((surrogateKey.map(_.surrogateID).toSet ++ attributes.map(_.surrogateID).toSet).size == surrogateKey.size + attributes.size)

  def writeToStandardFile() = {
    val file = DBSynthesis_IOService.getBCNFTableSchemaFile(id)
    toJsonFile(file)
  }

}


object BCNFTableSchema extends JsonReadable[BCNFTableSchema]{

  def loadAllBCNFTableSchemata(subdomain: String, originalID: String) = {
    val dir = DBSynthesis_IOService.getBCNFTableSchemaDir(subdomain,originalID)
    val ids = dir.listFiles().map(f => DecomposedTemporalTableIdentifier.fromFilename(f.getName))
    ids.map(id => load(id))
  }

  def load(id:DecomposedTemporalTableIdentifier) = {
    val file = DBSynthesis_IOService.getBCNFTableSchemaFile(id)
    BCNFTableSchema.fromJsonFile(file.getAbsolutePath)
  }

  def filterNotFullyDecomposedTables(subdomain:String,viewIds: collection.IndexedSeq[String]) = {
    val subdomainIdsWithDTT = viewIds
      .filter(id => DBSynthesis_IOService.decomposedTemporalTablesExist(subdomain, id))
    val schemata = subdomainIdsWithDTT.map(id => TemporalSchema.load(id)).map(ts => (ts.id,ts)).toMap
    val filteredSecondStep = subdomainIdsWithDTT.filter(id => {
      val dtts = loadAllBCNFTableSchemata(subdomain,id)
      val attrIds = dtts.flatMap(_.attributes.map(_.referencedAttrId)).toSet
      val originalSchema = schemata(id)
      //val missing = originalSchema.attributes.map(_.attrId).toSet.diff(attrIds)
      attrIds!= originalSchema.attributes.map(_.attrId).toSet
    })
    subdomainIdsWithDTT.diff(filteredSecondStep)
  }
}