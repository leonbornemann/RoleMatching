package de.hpi.role_matching.cbrm.evidence_based_weighting

import de.hpi.data_preparation.socrata.tfmp_input.table.TemporalFieldTrait
import de.hpi.role_matching.cbrm.compatibility_graph.role_tree.RoleReference

trait EdgeScore[A] {

  def name: String

  def compute(tr1: RoleReference[A], tr2: RoleReference[A]): Double = compute(tr1.getDataTuple.head, tr2.getDataTuple.head)

  def compute(tr1: RoleReference[A]): Double

  def compute(f1: TemporalFieldTrait[A], f2: TemporalFieldTrait[A]): Double

}
