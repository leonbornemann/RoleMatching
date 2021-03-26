package de.hpi.tfm.data.tfmp_input.association

import de.hpi.tfm.data.socrata.change.temporal_tables.attribute.SurrogateAttributeLineage
import de.hpi.tfm.data.socrata.metadata.custom.schemaHistory.AttributeLineageWithHashMap
import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}

case class AssociationSchemaHelper(id: AssociationIdentifier, surrogateKey: SurrogateAttributeLineage, attributeLineage: AttributeLineageWithHashMap) extends JsonWritable[AssociationSchemaHelper]{

  def AssociationSchema = new AssociationSchema(id,surrogateKey,attributeLineage.toAttributeLineage)

}
object AssociationSchemaHelper extends JsonReadable[AssociationSchemaHelper]{

}