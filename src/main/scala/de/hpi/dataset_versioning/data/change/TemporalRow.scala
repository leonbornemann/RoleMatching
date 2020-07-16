package de.hpi.dataset_versioning.data.change

import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage

class TemporalRow(val entityID:Long,val fields:collection.IndexedSeq[ValueLineage]) {

}
