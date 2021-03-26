package de.hpi.tfm.compatibility.graph.association

import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.tfmp_input.table.nonSketch.ValueTransition

case class AssociationGraphEdge(v1: AssociationIdentifier,
                                v2: AssociationIdentifier,
                                summedEvidence: Int,
                                evidenceMultiSet: IndexedSeq[(ValueTransition[Any], Int)]) {

}
