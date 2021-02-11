package de.hpi.dataset_versioning.db_synthesis.database.table

import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.{AttributeLineage, SurrogateAttributeLineage}
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.AttributeLineageWithHashMap
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.database.table.AssociationSchema.getAssociationSchemaFile
import de.hpi.dataset_versioning.io.DBSynthesis_IOService
import de.hpi.dataset_versioning.io.DBSynthesis_IOService.{ASSOCIATION_SCHEMA_DIR, createParentDirs}

import java.io.File

class AssociationSchema(val id:DecomposedTemporalTableIdentifier,
                            val surrogateKey:SurrogateAttributeLineage,
                            val attributeLineage: AttributeLineage) {
  def getStandardFilePath() = getAssociationSchemaFile(id).getAbsolutePath


  override def toString: String = id + s"(${surrogateKey}, ${attributeLineage})"

  def compositeID: String = id.compositeID


  def writeToStandardFile() = {
    val file = getAssociationSchemaFile(id)
    val helper = AssociationSchemaHelper(id,
      surrogateKey,
      AttributeLineageWithHashMap.from(attributeLineage))
    helper.toJsonFile(file)
  }

}
object AssociationSchema{

  def loadAllAssociations(subdomain: String, originalID: String) = {
    val dir = getAssociationSchemaDir(subdomain,originalID)
    val ids = dir.listFiles().map(f => DecomposedTemporalTableIdentifier.fromFilename(f.getName))
    ids.map(id => load(id))
  }

  def loadAllAssociationsInSubdomain(subdomain:String) = {
    val dirs = getAssociationSchemaParentDirs(subdomain)
    val viewIds = dirs.map(_.getName)
    viewIds.flatMap(viewID => loadAllAssociations(subdomain,viewID))
  }

  def load(id:DecomposedTemporalTableIdentifier) = {
    val file = getAssociationSchemaFile(id)
    val helper = AssociationSchemaHelper.fromJsonFile(file.getAbsolutePath)
    helper.AssociationSchema
  }

  def getAssociationSchemaDir(subdomain: String, viewID: String) = createParentDirs(new File(s"$ASSOCIATION_SCHEMA_DIR/$subdomain/$viewID/"))
  def getAssociationSchemaParentDirs(subdomain:String) = {
    createParentDirs(new File(s"$ASSOCIATION_SCHEMA_DIR/$subdomain/")).listFiles()
  }

  def getAssociationSchemaFile(id:DecomposedTemporalTableIdentifier) = {
    assert(id.associationID.isDefined)
    val topDir = ASSOCIATION_SCHEMA_DIR
    createParentDirs(new File(s"$topDir/${id.subdomain}/${id.viewID}/${id.compositeID}.json"))
  }

  def associationSchemataExist(subdomain: String, id: String) = {
    val dir = getAssociationSchemaDir(subdomain, id)
    dir.exists() && !dir.listFiles().isEmpty
  }
}
