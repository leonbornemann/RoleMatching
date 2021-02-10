package de.hpi.dataset_versioning.db_synthesis.database.table

import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.{AttributeLineage, SurrogateAttributeLineage}
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.AttributeLineageWithHashMap
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

class AssociationSchema(val id:DecomposedTemporalTableIdentifier,
                            val surrogateKey:SurrogateAttributeLineage,
                            val attributeLineage: AttributeLineage) {
  def getStandardFilePath() = DBSynthesis_IOService.getAssociationSchemaFile(id).getAbsolutePath


  override def toString: String = id + s"(${surrogateKey}, ${attributeLineage})"

  def compositeID: String = id.compositeID


  def writeToStandardFile() = {
    val file = DBSynthesis_IOService.getAssociationSchemaFile(id)
    val helper = AssociationSchemaHelper(id,
      surrogateKey,
      AttributeLineageWithHashMap.from(attributeLineage))
    helper.toJsonFile(file)
  }

}
object AssociationSchema{

  def loadAllAssociations(subdomain: String, originalID: String) = {
    val dir = DBSynthesis_IOService.getAssociationSchemaDir(subdomain,originalID)
    val ids = dir.listFiles().map(f => DecomposedTemporalTableIdentifier.fromFilename(f.getName))
    ids.map(id => load(id))
  }

  def loadAllAssociationsInSubdomain(subdomain:String) = {
    val dirs = DBSynthesis_IOService.getAssociationSchemaParentDirs(subdomain)
    val viewIds = dirs.map(_.getName)
    viewIds.flatMap(viewID => loadAllAssociations(subdomain,viewID))
  }

  def load(id:DecomposedTemporalTableIdentifier) = {
    val file = DBSynthesis_IOService.getAssociationSchemaFile(id)
    val helper = AssociationSchemaHelper.fromJsonFile(file.getAbsolutePath)
    helper.AssociationSchema
  }

}
