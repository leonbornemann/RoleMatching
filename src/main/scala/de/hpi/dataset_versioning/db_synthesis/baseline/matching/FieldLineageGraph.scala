package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import de.hpi.dataset_versioning.db_synthesis.preparation.{FieldLineageGraphEdge, FieldLineageMergeabilityGraph}

import scala.collection.mutable

class FieldLineageGraph[A] {

  def toFieldLineageMergeabilityGraph(includeEvidenceSet:Boolean=false) = {
    FieldLineageMergeabilityGraph(edges.toIndexedSeq.map(e => {
      var evidenceSet:Option[collection.Set[(Any,Any)]] = None
      if(includeEvidenceSet) {
        val tupA = e.tupleReferenceA.getDataTuple.head
        val tupB = e.tupleReferenceA.getDataTuple.head
        evidenceSet = Some(tupA.getOverlapEvidenceSet(tupB).map(_.asInstanceOf[(Any,Any)]))
      }
      FieldLineageGraphEdge(e.tupleReferenceA.toIDBasedTupleReference, e.tupleReferenceB.toIDBasedTupleReference, e.evidence,evidenceSet)
    }))
  }

  val edges = mutable.HashSet[General_1_to_1_TupleMatching[A]]()

  def getTupleMatchOption(ref1:TupleReference[A], ref2:TupleReference[A]) = {
    val mappedFieldLineages = buildTuples(ref1, ref2) // this is a map with all LHS being fields from tupleA and all rhs being fields from tuple B
    val evidence = mappedFieldLineages._1.getOverlapEvidenceCount(mappedFieldLineages._2)
    if (evidence == -1) {
      None
    } else {
      Some(General_1_to_1_TupleMatching(ref1,ref2, evidence))
    }
  }

  def buildTuples(ref1: TupleReference[A], ref2: TupleReference[A]) = {
    val lineages1 = ref1.getDataTuple
    val lineages2 = ref2.getDataTuple
    assert(lineages1.size==1 && lineages2.size==1)
    (lineages1.head,lineages2.head)
  }


}
