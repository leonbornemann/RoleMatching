package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier

case class AssociationGraphEdge(firstMatchPartner: DecomposedTemporalTableIdentifier,
                                secondMatchPartner: DecomposedTemporalTableIdentifier,
                                evidence: Int,
                                minChangeBenefit: Int,
                                maxChangeBenefit: Int) extends JsonWritable[AssociationGraphEdge]{

}

object AssociationGraphEdge extends JsonReadable[AssociationGraphEdge]
