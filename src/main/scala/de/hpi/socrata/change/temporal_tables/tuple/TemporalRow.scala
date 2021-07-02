package de.hpi.socrata.change.temporal_tables.tuple

import de.hpi.socrata.tfmp_input.table.nonSketch.FactLineage

class TemporalRow(val entityID:Long,val fields:collection.IndexedSeq[FactLineage]) extends Serializable{
  private def serialVersionUID = 42L

}
object TemporalRow{
}
