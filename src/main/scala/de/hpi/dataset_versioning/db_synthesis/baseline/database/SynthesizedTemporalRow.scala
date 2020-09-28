package de.hpi.dataset_versioning.db_synthesis.baseline.database

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalRow
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage

import scala.collection.mutable

class SynthesizedTemporalRow(entityID: Long,
                             fields: collection.IndexedSeq[ValueLineage],
                             val tupleIDToDTTTupleID:mutable.HashMap[DecomposedTemporalTableIdentifier,Long],
                             val tupleIDTOViewTupleIDs:mutable.HashMap[String,mutable.HashSet[Long]]) extends TemporalRow(entityID,fields){

}
