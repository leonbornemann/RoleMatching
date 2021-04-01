package de.hpi.tfm.compatibility.graph.fact

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.data.tfmp_input.table.nonSketch.ValueTransition

import scala.collection.mutable

trait FactMatchCreator[A] {

  def getGraphConfig: GraphConfig

  def toFieldLineageMergeabilityGraph(includeEvidenceSet:Boolean=false) = {
    FactMergeabilityGraph(facts.toIndexedSeq.map(e => {
      var evidenceSet:Option[collection.IndexedSeq[(ValueTransition[Any],Int)]] = None
      if(includeEvidenceSet) {
        val tupA = e.tupleReferenceA.getDataTuple.head
        val tupB = e.tupleReferenceB.getDataTuple.head
        evidenceSet = Some(tupA.getOverlapEvidenceMultiSet(tupB).toIndexedSeq.map(t => (t._1.asInstanceOf[ValueTransition[Any]],t._2)))
        assert(evidenceSet.get.map(_._2).sum==e.evidence)
      }
      FactMergeabilityGraphEdge(e.tupleReferenceA.toIDBasedTupleReference,
        e.tupleReferenceB.toIDBasedTupleReference,
        e.evidence,
        evidenceSet)
    }),getGraphConfig)
  }

  val facts = mutable.HashSet[FactMatch[A]]()

  def getTupleMatchOption(ref1:TupleReference[A], ref2:TupleReference[A]) = {
    val left = ref1.getDataTuple.head
    val right = ref2.getDataTuple.head // this is a map with all LHS being fields from tupleA and all rhs being fields from tuple B
    val evidence = left.getOverlapEvidenceCount(right)
    if (evidence == -1) {
      None
    } else {
      Some(FactMatch(ref1,ref2, evidence))
    }
  }


}
