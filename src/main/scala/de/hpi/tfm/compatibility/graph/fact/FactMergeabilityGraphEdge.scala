package de.hpi.tfm.compatibility.graph.fact

import de.hpi.tfm.data.tfmp_input.table.nonSketch.ValueTransition

case class FactMergeabilityGraphEdge(tupleReferenceA: IDBasedTupleReference,
                                     tupleReferenceB: IDBasedTupleReference,
                                     var evidence: Int,
                                     evidenceSet: Option[collection.IndexedSeq[(ValueTransition[Any], Int)]] = None) {
  if (evidenceSet.isDefined) {
    if (evidence != evidenceSet.get.map(_._2).sum) {
      println(this)
    }
    assert(evidence == evidenceSet.get.map(_._2).sum)
  }

}
