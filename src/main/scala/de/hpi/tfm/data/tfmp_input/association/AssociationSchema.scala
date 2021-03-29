package de.hpi.tfm.data.tfmp_input.association

import de.hpi.tfm.data.socrata.change.temporal_tables.attribute.{AttributeLineage, SurrogateAttributeLineage}
import de.hpi.tfm.data.socrata.metadata.custom.schemaHistory.AttributeLineageWithHashMap
import de.hpi.tfm.data.tfmp_input.association.AssociationSchema.getAssociationSchemaFile
import de.hpi.tfm.io.DBSynthesis_IOService.{ASSOCIATION_SCHEMA_DIR, createParentDirs}

import java.io.File

class AssociationSchema(val id:AssociationIdentifier,
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
    val ids = dir.listFiles().map(f => AssociationIdentifier.fromFilename(f.getName))
    ids.map(id => load(id))
  }

  def loadAllAssociationsInSubdomain(subdomain:String) = {
    val dirs = getAssociationSchemaParentDirs(subdomain)
    val viewIds = dirs.map(_.getName)
    viewIds.flatMap(viewID => loadAllAssociations(subdomain,viewID))
  }

  def load(id:AssociationIdentifier) = {
    val file = getAssociationSchemaFile(id)
    val helper = AssociationSchemaHelper.fromJsonFile(file.getAbsolutePath)
    helper.AssociationSchema
  }

  def getAssociationSchemaDir(subdomain: String, viewID: String) = createParentDirs(new File(s"${ASSOCIATION_SCHEMA_DIR(subdomain)}/$viewID/"))
  def getAssociationSchemaParentDirs(subdomain:String) = {
    println(new File(ASSOCIATION_SCHEMA_DIR(subdomain)).getAbsolutePath)
    createParentDirs(new File(ASSOCIATION_SCHEMA_DIR(subdomain))).listFiles()
  }

  def getAssociationSchemaFile(id:AssociationIdentifier) = {
    assert(id.associationID.isDefined)
    val topDir = ASSOCIATION_SCHEMA_DIR(id.subdomain)
    createParentDirs(new File(s"$topDir/${id.viewID}/${id.compositeID}.json"))
  }

  def associationSchemataExist(subdomain: String, id: String) = {
    val dir = getAssociationSchemaDir(subdomain, id)
    dir.exists() && !dir.listFiles().isEmpty
  }
}