package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

import de.hpi.role_matching.cbrm.data.json_serialization.{JsonReadable, JsonWritable}
import de.hpi.role_matching.cbrm.data.{RoleReference, ValueTransition}

case class CompatibilityGraphEdge(tupleReferenceA: RoleReference,
                                  tupleReferenceB: RoleReference,
                                  var evidence: Int,
                                  evidenceSet: Option[collection.IndexedSeq[(ValueTransition, Int)]] = None) extends JsonWritable[CompatibilityGraphEdge]{

  if (evidenceSet.isDefined) {
    if (evidence != evidenceSet.get.map(_._2).sum) {
      println(this)
    }
    assert(evidence == evidenceSet.get.map(_._2).sum)
  }
}

object CompatibilityGraphEdge extends JsonReadable[CompatibilityGraphEdge]{

}
