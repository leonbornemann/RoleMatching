package de.hpi.dataset_versioning.data.change.temporal_tables

import java.time.LocalDate

@SerialVersionUID(3L)
case class SurrogateAttributeLineage(surrogateID: Int, attrId: Int, insert: LocalDate) extends Serializable{

  override def toString: String = s"SURROGATE_ID[$surrogateID]"

}
