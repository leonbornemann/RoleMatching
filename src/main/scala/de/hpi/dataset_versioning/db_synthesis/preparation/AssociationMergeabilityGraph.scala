package de.hpi.dataset_versioning.db_synthesis.preparation

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

case class AssociationMergeabilityGraph(edges: IndexedSeq[AssociationMergeabilityGraphEdge]) extends JsonWritable[AssociationMergeabilityGraph]{

  def writeToStandardFile(subdomain:String) = {
    DBSynthesis_IOService.getAssociationMergeabilityGraphFile(subdomain)
  }

}
object AssociationMergeabilityGraph extends JsonReadable[AssociationMergeabilityGraph]{

  def readFromStandardFile(subdomain:String) = {
    fromJsonFile(DBSynthesis_IOService.getAssociationMergeabilityGraphFile(subdomain).getAbsolutePath)
  }

}
