package de.hpi.role_matching.blocking.cbrb.role_tree

import de.hpi.role_matching.data.json_serialization.{JsonReadable, JsonWritable}
import de.hpi.role_matching.data.{RoleReference, ValueTransition}

case class FullyCompatibleCandidate(tupleReferenceA: RoleReference,
                                    tupleReferenceB: RoleReference,
                                    var evidence: Int,
                                    evidenceSet: Option[collection.IndexedSeq[(ValueTransition, Int)]] = None) extends JsonWritable[FullyCompatibleCandidate]{

  if (evidenceSet.isDefined) {
    if (evidence != evidenceSet.get.map(_._2).sum) {
      println(this)
    }
    assert(evidence == evidenceSet.get.map(_._2).sum)
  }
}

object FullyCompatibleCandidate extends JsonReadable[FullyCompatibleCandidate]{

}
