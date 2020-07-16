package de.hpi.dataset_versioning.db_synthesis.top_down_no_change.merge.measures

import scala.collection.mutable.ArrayBuffer

case class FieldMapping(attributeMappings: ArrayBuffer[DatasetAttributeMapping], tupleMappings: ArrayBuffer[DatasetRowIDMapping]) {

}
