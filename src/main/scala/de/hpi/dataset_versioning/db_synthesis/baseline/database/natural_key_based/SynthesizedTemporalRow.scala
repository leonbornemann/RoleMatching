package de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based

import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.{TemporalRow, ValueLineage}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier

import scala.collection.mutable

class SynthesizedTemporalRow(entityID: Long,
                             fields: collection.IndexedSeq[ValueLineage],
                             val tupleIDToDTTTupleID:mutable.HashMap[DecomposedTemporalTableIdentifier,Long],
                             val tupleIDTOViewTupleIDs:mutable.HashMap[String,mutable.HashSet[Long]]) extends TemporalRow(entityID,fields){

}
