package de.hpi.dataset_versioning.db_synthesis.database.table

import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.SurrogateAttributeLineage
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.AttributeLineageWithHashMap
import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier

case class AssociationSchemaHelper(id: DecomposedTemporalTableIdentifier, surrogateKey: SurrogateAttributeLineage, attributeLineage: AttributeLineageWithHashMap) extends JsonWritable[AssociationSchemaHelper]{

  def AssociationSchema = new AssociationSchema(id,surrogateKey,attributeLineage.toAttributeLineage)

}
object AssociationSchemaHelper extends JsonReadable[AssociationSchemaHelper]{

}
