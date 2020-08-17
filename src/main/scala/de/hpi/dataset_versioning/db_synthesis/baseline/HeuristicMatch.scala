package de.hpi.dataset_versioning.db_synthesis.baseline

import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTable

class HeuristicMatch(val firstMatchPartner:SynthesizedTemporalDatabaseTable,
                     val secondMatchPartner:SynthesizedTemporalDatabaseTable,
                     val score:Int) {

}
