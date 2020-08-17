package de.hpi.dataset_versioning.db_synthesis.baseline

import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage

class ComputedMatch(firstMatchPartner:SynthesizedTemporalDatabaseTable,
                    secondMatchPartner:SynthesizedTemporalDatabaseTable,
                    score:Int,
                    attributeLineageMapping:Map[AttributeLineage,AttributeLineage]) extends HeuristicMatch(firstMatchPartner,secondMatchPartner,score) {

}
