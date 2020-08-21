package de.hpi.dataset_versioning.db_synthesis.baseline.heuristics

import de.hpi.dataset_versioning.db_synthesis.baseline.{HeuristicMatch, SynthesizedTemporalDatabaseTable}

trait HeursticMatchCalculator {

  def calculateMatch(tableA: SynthesizedTemporalDatabaseTable, tableB: SynthesizedTemporalDatabaseTable): HeuristicMatch
}
