package de.hpi.dataset_versioning.db_synthesis.baseline.heuristics

import de.hpi.dataset_versioning.db_synthesis.baseline.{SynthesizedTemporalDatabaseTable, TableUnionMatch}
import de.hpi.dataset_versioning.db_synthesis.sketches.SynthesizedTemporalDatabaseTableSketch

trait HeursticMatchCalculator {

  def calculateMatch(tableA: SynthesizedTemporalDatabaseTableSketch, tableB: SynthesizedTemporalDatabaseTableSketch): TableUnionMatch
}
