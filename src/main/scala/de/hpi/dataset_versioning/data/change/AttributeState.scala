package de.hpi.dataset_versioning.data.change

import de.hpi.dataset_versioning.data.simplified.Attribute

case class AttributeState(attr:Option[Attribute]) {
  def isNE: Boolean = !attr.isDefined

  def displayName = if(attr.isDefined) attr.get.name else ReservedChangeValues.NE_DISPLAY

}
