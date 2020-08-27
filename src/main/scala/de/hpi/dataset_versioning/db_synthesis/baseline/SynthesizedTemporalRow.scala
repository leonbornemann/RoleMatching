package de.hpi.dataset_versioning.db_synthesis.baseline

import de.hpi.dataset_versioning.data.change.temporal_tables.{ProjectedTemporalRow, TemporalRow}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage

import scala.collection.mutable

class SynthesizedTemporalRow(entityID: Long,
                             fields: collection.IndexedSeq[ValueLineage],
                             tupleIDToDTTTupleID:mutable.HashMap[DecomposedTemporalTableIdentifier,Long]) extends TemporalRow(entityID,fields){

}
