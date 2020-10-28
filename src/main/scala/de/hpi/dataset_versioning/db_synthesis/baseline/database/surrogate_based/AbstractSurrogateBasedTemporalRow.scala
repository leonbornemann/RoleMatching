package de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based

import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

@SerialVersionUID(3L)
class AbstractSurrogateBasedTemporalRow[T](val keys: IndexedSeq[Int], val value: TemporalFieldTrait[T],val foreignKeys: IndexedSeq[Int]) extends Serializable{

}
