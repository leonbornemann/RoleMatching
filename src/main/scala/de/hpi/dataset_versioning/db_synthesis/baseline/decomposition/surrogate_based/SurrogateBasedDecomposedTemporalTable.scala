package de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.surrogate_based

import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.{AttributeLineage, SurrogateAttributeLineage}
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.{AttributeLineageWithHashMap, TemporalSchema}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.database.GlobalSurrogateRegistry
import de.hpi.dataset_versioning.db_synthesis.database.table.{AssociationSchema, BCNFTableSchema}
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

import scala.collection.mutable.ArrayBuffer

@SerialVersionUID(3L)
class SurrogateBasedDecomposedTemporalTable(val id: DecomposedTemporalTableIdentifier,
                                            val surrogateKey: IndexedSeq[SurrogateAttributeLineage],
                                            val attributes: ArrayBuffer[AttributeLineage],
                                            val foreignSurrogateKeysToReferencedTables: IndexedSeq[(SurrogateAttributeLineage, collection.IndexedSeq[DecomposedTemporalTableIdentifier])])  extends Serializable{
  def allSurrogates: Set[SurrogateAttributeLineage] = surrogateKey.toSet ++ foreignSurrogateKeysToReferencedTables.map(_._1).toSet

  def isAssociation: Boolean = attributes.size==1

  def getSchemaString = {
    id.viewID + "_" +id.bcnfID + "(" +
      surrogateKey.map(pk => pk.surrogateID).mkString(",") + "  ->  " +
      attributes.toIndexedSeq.map(nk => nk.lastName  + s"[${nk.attrId}]").sorted.mkString(",") + "   "
      foreignSurrogateKeysToReferencedTables.map(reference => reference._1.surrogateID).mkString(",") + ")"
  }

  def compositeID: String = id.compositeID

  def getReferenceSkeleton() = {
    new SurrogateBasedDecomposedTemporalTable(new DecomposedTemporalTableIdentifier(id.subdomain,id.viewID,id.bcnfID,id.associationID),
      surrogateKey,
      ArrayBuffer(),
      foreignSurrogateKeysToReferencedTables)
  }

  def furtherDecomposeToAssociations = {
    val associationTableIds = scala.collection.mutable.HashSet() ++ ((0 until attributes.size)
      .map(i => DecomposedTemporalTableIdentifier(id.subdomain,id.viewID,id.bcnfID,Some(i))))
    val associations = attributes.zip(associationTableIds).map{case (rhs,associationTableID) => new AssociationSchema(associationTableID,
      new SurrogateAttributeLineage(GlobalSurrogateRegistry.getNextFreeSurrogateID,rhs.attrId,rhs.lineage.firstKey),
      rhs)}
    val newReferenceBasedBCNFTable = BCNFTableSchema(id,
    surrogateKey,
    associations.map(_.surrogateKey),
    foreignSurrogateKeysToReferencedTables)
    (newReferenceBasedBCNFTable,associations)
  }

  def writeToStandardFile() = {
    val file = DBSynthesis_IOService.getSurrogateBasedDecomposedTemporalTableFile(id)
    val helper = SurrogateBasedDecomposedTemporalTableHelper(id,
      surrogateKey,
      attributes.map(AttributeLineageWithHashMap.from(_)),
      foreignSurrogateKeysToReferencedTables)
    helper.toJsonFile(file)
  }
}
object SurrogateBasedDecomposedTemporalTable{

  def loadAllDecomposedTemporalTables(subdomain: String, originalID: String) = {
    val dir = DBSynthesis_IOService.getSurrogateBasedDecomposedTemporalTableDir(subdomain,originalID)
    val ids = dir.listFiles().map(f => DecomposedTemporalTableIdentifier.fromFilename(f.getName))
    ids.map(id => load(id))
  }

  def load(id:DecomposedTemporalTableIdentifier) = {
    val file = DBSynthesis_IOService.getSurrogateBasedDecomposedTemporalTableFile(id)
    val helper = SurrogateBasedDecomposedTemporalTableHelper.fromJsonFile(file.getAbsolutePath)
    helper.toSurrogateBasedDecomposedTemporalTable
  }

  def filterNotFullyDecomposedTables(subdomain:String,viewIds: collection.IndexedSeq[String]) = {
    val subdomainIdsWithDTT = viewIds
      .filter(id => DBSynthesis_IOService.decomposedTemporalTablesExist(subdomain, id))
    val schemata = subdomainIdsWithDTT.map(id => TemporalSchema.load(id)).map(ts => (ts.id,ts)).toMap
    val filteredSecondStep = subdomainIdsWithDTT.filter(id => {
      val dtts = SurrogateBasedDecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain,id)
      val attrIds = dtts.flatMap(_.attributes.map(_.attrId)).toSet
      val originalSchema = schemata(id)
      //val missing = originalSchema.attributes.map(_.attrId).toSet.diff(attrIds)
      attrIds!= originalSchema.attributes.map(_.attrId).toSet
    })
    subdomainIdsWithDTT.diff(filteredSecondStep)
  }
}
