package de.hpi.tfm.compatibility.graph.fact

import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.data.tfmp_input.table.nonSketch.ValueTransition
import de.hpi.tfm.io.DBSynthesis_IOService

import java.io.File

case class FactMergeabilityGraphEdge(tupleReferenceA: IDBasedTupleReference,
                                     tupleReferenceB: IDBasedTupleReference,
                                     var evidence: Int,
                                     evidenceSet: Option[collection.IndexedSeq[(ValueTransition[Any], Int)]] = None) extends JsonWritable[FactMergeabilityGraphEdge]{
  if (evidenceSet.isDefined) {
    if (evidence != evidenceSet.get.map(_._2).sum) {
      println(this)
    }
    assert(evidence == evidenceSet.get.map(_._2).sum)
  }
}

object FactMergeabilityGraphEdge extends JsonReadable[FactMergeabilityGraphEdge]{

  def getEdgeCandidateJsonPerLineFile(datasetName:String) = {
    DBSynthesis_IOService.createParentDirs(new File(DBSynthesis_IOService.COMPATIBILITY_GRAPH_DIR + s"/$datasetName.json"))
  }
}
