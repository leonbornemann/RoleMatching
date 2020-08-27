package de.hpi.dataset_versioning.data.change.temporal_tables

import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage

class ProjectedTemporalRow(entityID:Long,
                           fields:collection.IndexedSeq[ValueLineage],
                           val mappedEntityIds:Set[Long]) extends TemporalRow(entityID,fields) {

}
