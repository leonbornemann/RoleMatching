package de.hpi.role_matching.cbrm.evidence_based_weighting

import de.hpi.role_matching.cbrm.data.RoleLineage

trait EdgeScore {

  def name: String

  def compute(f1: RoleLineage, f2: RoleLineage): Double

}
