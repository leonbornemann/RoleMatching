package de.hpi.dataset_versioning.data.change.temporal_tables.tuple

class ProjectedTemporalRow(entityID:Long,
                           fields:collection.IndexedSeq[ValueLineage],
                           val mappedEntityIds:Set[Long]) extends TemporalRow(entityID,fields) {

}
