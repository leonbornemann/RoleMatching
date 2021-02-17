package de.hpi.dataset_versioning.db_synthesis.graph.field_lineage

import de.hpi.dataset_versioning.db_synthesis.baseline.matching.{IDBasedTupleReference, ValueTransition}

case class FieldLineageGraphEdge(tupleReferenceA: IDBasedTupleReference,
                                 tupleReferenceB: IDBasedTupleReference,
                                 var evidence: Int,
                                 evidenceSet: Option[collection.IndexedSeq[(ValueTransition, Int)]] = None) {
  if (evidenceSet.isDefined) {
    if (evidence != evidenceSet.get.map(_._2).sum) {
      println(this)
    }
    assert(evidence == evidenceSet.get.map(_._2).sum)
  }

}
