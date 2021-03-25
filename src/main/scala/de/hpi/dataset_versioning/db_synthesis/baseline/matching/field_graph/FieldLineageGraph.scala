package de.hpi.dataset_versioning.db_synthesis.baseline.matching.field_graph

import de.hpi.dataset_versioning.data.change.ReservedChangeValues
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.{General_1_to_1_TupleMatching, TupleReference, ValueTransition}
import de.hpi.dataset_versioning.db_synthesis.graph.field_lineage.{FieldLineageGraphEdge, FieldLineageMergeabilityGraph}

import scala.collection.mutable

class FieldLineageGraph[A] {

  def toFieldLineageMergeabilityGraph(includeEvidenceSet:Boolean=false) = {
    FieldLineageMergeabilityGraph(edges.toIndexedSeq.map(e => {
      var evidenceSet:Option[collection.IndexedSeq[(ValueTransition[Any],Int)]] = None
      if(includeEvidenceSet) {
        val tupA = e.tupleReferenceA.getDataTuple.head
        val tupB = e.tupleReferenceB.getDataTuple.head
        evidenceSet = Some(tupA.getOverlapEvidenceMultiSet(tupB).toIndexedSeq.map(t => (t._1.asInstanceOf[ValueTransition[Any]],t._2)))
        assert(evidenceSet.get.map(_._2).sum==e.evidence)
      }
      FieldLineageGraphEdge(e.tupleReferenceA.toIDBasedTupleReference,
        e.tupleReferenceB.toIDBasedTupleReference,
        e.evidence,
        evidenceSet)
    }))
  }

  val edges = mutable.HashSet[General_1_to_1_TupleMatching[A]]()

  def getTupleMatchOption(ref1:TupleReference[A], ref2:TupleReference[A]) = {
    val left = ref1.getDataTuple.head
    val right = ref2.getDataTuple.head // this is a map with all LHS being fields from tupleA and all rhs being fields from tuple B
    val evidence = left.getOverlapEvidenceCount(right)
    if (evidence == -1) {
      None
    } else {
      Some(General_1_to_1_TupleMatching(ref1,ref2, evidence))
    }
  }


}
