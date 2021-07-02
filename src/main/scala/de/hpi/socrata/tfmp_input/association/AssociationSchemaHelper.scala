package de.hpi.socrata.tfmp_input.association

import de.hpi.socrata.change.temporal_tables.attribute.SurrogateAttributeLineage
import de.hpi.socrata.metadata.custom.schemaHistory.AttributeLineageWithHashMap
import de.hpi.socrata.{JsonReadable, JsonWritable}

case class AssociationSchemaHelper(id: AssociationIdentifier, surrogateKey: SurrogateAttributeLineage, attributeLineage: AttributeLineageWithHashMap) extends JsonWritable[AssociationSchemaHelper]{

  def AssociationSchema = new AssociationSchema(id,surrogateKey,attributeLineage.toAttributeLineage)

}
object AssociationSchemaHelper extends JsonReadable[AssociationSchemaHelper]{

}