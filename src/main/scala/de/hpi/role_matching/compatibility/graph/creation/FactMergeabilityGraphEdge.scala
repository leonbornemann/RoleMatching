package de.hpi.role_matching.compatibility.graph.creation

import de.hpi.socrata.io.Socrata_Synthesis_IOService
import de.hpi.socrata.tfmp_input.table.nonSketch.ValueTransition
import de.hpi.socrata.{JsonReadable, JsonWritable}

import java.io.File

case class FactMergeabilityGraphEdge(tupleReferenceA: IDBasedTupleReference,
                                     tupleReferenceB: IDBasedTupleReference,
                                     var evidence: Int,
                                     evidenceSet: Option[collection.IndexedSeq[(ValueTransition[Any], Int)]] = None) extends JsonWritable[FactMergeabilityGraphEdge]{
  def toWikipediaStyleStatRow() = {

  }

  if (evidenceSet.isDefined) {
    if (evidence != evidenceSet.get.map(_._2).sum) {
      println(this)
    }
    assert(evidence == evidenceSet.get.map(_._2).sum)
  }
}

object FactMergeabilityGraphEdge extends JsonReadable[FactMergeabilityGraphEdge]{

  def getEdgeCandidateJsonPerLineFile(subdomain:String) = {
    Socrata_Synthesis_IOService.createParentDirs(new File(Socrata_Synthesis_IOService.COMPATIBILITY_GRAPH_DIR(subdomain) + s"/$subdomain.json"))
  }
}
