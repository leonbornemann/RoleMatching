package de.hpi.dataset_versioning.data.change.temporal_tables.attribute

import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier

import java.time.LocalDate

@SerialVersionUID(3L)
case class SurrogateAttributeLineage(surrogateID: Int, referencedAttrId: Int) extends Serializable {

  override def toString: String = s"SK$surrogateID"

}
