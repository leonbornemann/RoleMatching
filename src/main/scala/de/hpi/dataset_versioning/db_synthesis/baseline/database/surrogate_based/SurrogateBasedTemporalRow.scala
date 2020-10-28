package de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based

import de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based.SurrogateBasedTemporalRowSketch
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.sketches.field.Variant2Sketch

@SerialVersionUID(3L)
class SurrogateBasedTemporalRow(keys:IndexedSeq[Int],val valueLineage:ValueLineage,foreignKeys:IndexedSeq[Int]) extends AbstractSurrogateBasedTemporalRow[Any](keys,valueLineage,foreignKeys) with Serializable{
  def toRowSketch = new SurrogateBasedTemporalRowSketch(keys,Variant2Sketch.fromValueLineage(valueLineage),foreignKeys)

}
