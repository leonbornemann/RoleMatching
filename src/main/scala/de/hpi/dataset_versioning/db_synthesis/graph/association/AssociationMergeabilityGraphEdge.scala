package de.hpi.dataset_versioning.db_synthesis.graph.association

import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.ValueTransition

case class AssociationMergeabilityGraphEdge(v1: DecomposedTemporalTableIdentifier,
                                            v2: DecomposedTemporalTableIdentifier,
                                            summedEvidence: Int,
                                            evidenceMultiSet: IndexedSeq[(ValueTransition, Int)]) {

}
