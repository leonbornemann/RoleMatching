package de.hpi.dataset_versioning.db_synthesis.top_down.merge.measures

import scala.collection.mutable.ArrayBuffer

case class FieldMapping(attributeMappings: ArrayBuffer[DatasetAttributeMapping], tupleMappings: ArrayBuffer[DatasetRowIDMapping]) {

}
