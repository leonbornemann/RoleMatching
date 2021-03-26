package de.hpi.tfm.compatibility.graph.association

import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier

case class AssociationGraphEdgeCandidate(firstMatchPartner: AssociationIdentifier,
                                         secondMatchPartner: AssociationIdentifier,
                                         evidence: Int,
                                         minChangeBenefit: Int,
                                         maxChangeBenefit: Int) extends JsonWritable[AssociationGraphEdgeCandidate]{

}

object AssociationGraphEdgeCandidate extends JsonReadable[AssociationGraphEdgeCandidate]
