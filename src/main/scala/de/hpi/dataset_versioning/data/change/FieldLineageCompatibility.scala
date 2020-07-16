package de.hpi.dataset_versioning.data.change

trait FieldLineageCompatibility {

  def isCompatible(fl1:FieldLineageReference,fl2:FieldLineageReference):Boolean
}
