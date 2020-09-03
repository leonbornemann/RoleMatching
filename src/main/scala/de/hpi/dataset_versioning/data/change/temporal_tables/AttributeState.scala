package de.hpi.dataset_versioning.data.change.temporal_tables

import de.hpi.dataset_versioning.data.change.ReservedChangeValues
import de.hpi.dataset_versioning.data.simplified.Attribute

@SerialVersionUID(3L)
case class AttributeState(attr:Option[Attribute]) extends Serializable{
  def exists: Boolean = attr.isDefined

  def isNE: Boolean = !attr.isDefined

  def displayName = if(attr.isDefined) attr.get.name else ReservedChangeValues.NE_DISPLAY

}

object AttributeState {
  val NON_EXISTANT = AttributeState(None)
}
