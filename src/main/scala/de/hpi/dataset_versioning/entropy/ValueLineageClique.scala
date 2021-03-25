package de.hpi.dataset_versioning.entropy

import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.sketches.field.{AbstractTemporalField, TemporalFieldTrait}

case class ValueLineageClique(clique: IndexedSeq[ValueLineage]) {

  def averageNewScore = {
    if(clique.size==1){
      0
    } else {
      clique.map(v => {
        v.newScore(merge)
      }).sum / clique.size.toDouble
    }
  }


  def averageMutualInformation = {
    if(clique.size==1){
      0
    } else {
      clique.map(v => {
        v.mutualInformation(merge)
      }).sum / clique.size.toDouble
    }
  }

  def entropyReduction = {
    if(clique.size==1){
      0
    } else {
      AbstractTemporalField.ENTROPY_REDUCTION_SET_FIELD(clique.map(_.asInstanceOf[TemporalFieldTrait[Any]]).toSet)
    }
  }

  val merge = ValueLineage.tryMergeAll(clique).get

  def averageEvidenceToMergedResultScore = {
    if(clique.size==1)
      0
    else
      clique.map(v => {
        v.getOverlapEvidenceCount(merge)
      }).sum / clique.size.toDouble
  }

}
