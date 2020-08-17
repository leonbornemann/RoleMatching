package de.hpi.dataset_versioning.data.change.temporal_tables

import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage

case class EntityFieldLineage(entityID: Long, lineage: ValueLineage) {

}
