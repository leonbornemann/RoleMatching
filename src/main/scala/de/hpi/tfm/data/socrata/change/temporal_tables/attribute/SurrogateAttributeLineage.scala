package de.hpi.tfm.data.socrata.change.temporal_tables.attribute

@SerialVersionUID(3L)
case class SurrogateAttributeLineage(surrogateID: Int, referencedAttrId: Int) extends Serializable {

  override def toString: String = s"SK$surrogateID"

}
