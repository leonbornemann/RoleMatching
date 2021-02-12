package de.hpi.dataset_versioning.db_synthesis.preparation

import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TupleReference

case class FieldLineageGraphEdge(tupleReferenceA:IDBasedTupleReference,
                                    tupleReferenceB:IDBasedTupleReference,
                                    var evidence:Int,
                                    evidenceSet:Option[collection.IndexedSeq[(ValueTransition,Int)]] = None) {

}
