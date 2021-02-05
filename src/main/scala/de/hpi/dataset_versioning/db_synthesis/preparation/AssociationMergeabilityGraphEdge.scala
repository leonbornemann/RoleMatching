package de.hpi.dataset_versioning.db_synthesis.preparation

import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier

case class AssociationMergeabilityGraphEdge(v1: DecomposedTemporalTableIdentifier, v2: DecomposedTemporalTableIdentifier, summedEvidence: Int) {

}
