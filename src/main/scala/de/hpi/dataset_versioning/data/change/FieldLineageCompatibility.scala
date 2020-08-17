package de.hpi.dataset_versioning.data.change

import de.hpi.dataset_versioning.data.change.temporal_tables.FieldLineageReference

trait FieldLineageCompatibility {

  def isCompatible(fl1:FieldLineageReference,fl2:FieldLineageReference):Boolean
}
