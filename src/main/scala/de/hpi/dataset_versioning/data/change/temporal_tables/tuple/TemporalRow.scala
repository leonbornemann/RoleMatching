package de.hpi.dataset_versioning.data.change.temporal_tables.tuple

class TemporalRow(val entityID:Long,val fields:collection.IndexedSeq[ValueLineage]) extends Serializable{
  private def serialVersionUID = 42L

}
object TemporalRow{
}
