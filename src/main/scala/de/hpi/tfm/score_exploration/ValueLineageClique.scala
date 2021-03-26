package de.hpi.tfm.score_exploration

import de.hpi.tfm.data.tfmp_input.table.{AbstractTemporalField, TemporalFieldTrait}
import de.hpi.tfm.data.tfmp_input.table.nonSketch.FactLineage

case class ValueLineageClique(clique: IndexedSeq[FactLineage]) {

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

  val merge = FactLineage.tryMergeAll(clique).get

  def averageEvidenceToMergedResultScore = {
    if(clique.size==1)
      0
    else
      clique.map(v => {
        v.getOverlapEvidenceCount(merge)
      }).sum / clique.size.toDouble
  }

}
