package de.hpi.tfm.data.socrata.change.temporal_tables.tuple

import de.hpi.tfm.data.tfmp_input.table.nonSketch.FactLineage

class TemporalRow(val entityID:Long,val fields:collection.IndexedSeq[FactLineage]) extends Serializable{
  private def serialVersionUID = 42L

}
object TemporalRow{
}
