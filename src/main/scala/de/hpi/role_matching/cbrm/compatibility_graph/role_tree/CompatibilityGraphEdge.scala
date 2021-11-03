package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

import de.hpi.data_preparation.socrata.io.Socrata_Synthesis_IOService
import de.hpi.data_preparation.socrata.tfmp_input.table.nonSketch.ValueTransition
import de.hpi.data_preparation.socrata.{JsonReadable, JsonWritable}

import java.io.File

case class CompatibilityGraphEdge(tupleReferenceA: IDBasedRoleReference,
                                  tupleReferenceB: IDBasedRoleReference,
                                  var evidence: Int,
                                  evidenceSet: Option[collection.IndexedSeq[(ValueTransition[Any], Int)]] = None) extends JsonWritable[CompatibilityGraphEdge]{
  def toWikipediaStyleStatRow() = {

  }

  if (evidenceSet.isDefined) {
    if (evidence != evidenceSet.get.map(_._2).sum) {
      println(this)
    }
    assert(evidence == evidenceSet.get.map(_._2).sum)
  }
}

object CompatibilityGraphEdge extends JsonReadable[CompatibilityGraphEdge]{

  def getEdgeCandidateJsonPerLineFile(subdomain:String) = {
    Socrata_Synthesis_IOService.createParentDirs(new File(Socrata_Synthesis_IOService.COMPATIBILITY_GRAPH_DIR(subdomain) + s"/$subdomain.json"))
  }
}
