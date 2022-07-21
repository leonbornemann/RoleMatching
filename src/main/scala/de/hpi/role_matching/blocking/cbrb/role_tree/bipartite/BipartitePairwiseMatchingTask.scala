package de.hpi.role_matching.blocking.cbrb.role_tree.bipartite

import de.hpi.role_matching.data.RoleReference

case class BipartitePairwiseMatchingTask(tuplesLeft: IndexedSeq[RoleReference],
                                         tuplesRight: IndexedSeq[RoleReference],
                                         intervalLeft: (Int, Int),
                                         intervalRight: (Int, Int),
                                        ) {
  def totalMatchChecks = tuplesLeft.size * tuplesRight.size

  def secondBorderEnd: Int = intervalRight._2

  def secondBorderStart = intervalRight._1

  def firstBorderEnd: Int = intervalLeft._2


  def firstBorderStart = intervalLeft._1
}
