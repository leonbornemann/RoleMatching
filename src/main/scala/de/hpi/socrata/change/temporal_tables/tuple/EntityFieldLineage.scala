package de.hpi.socrata.change.temporal_tables.tuple

import de.hpi.socrata.tfmp_input.table.nonSketch.FactLineage

case class EntityFieldLineage(entityID: Long, lineage: FactLineage) {

}
