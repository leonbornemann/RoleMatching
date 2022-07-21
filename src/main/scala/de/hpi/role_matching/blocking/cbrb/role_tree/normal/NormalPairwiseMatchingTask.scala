package de.hpi.role_matching.blocking.cbrb.role_tree.normal

import de.hpi.role_matching.data.RoleReference

case class NormalPairwiseMatchingTask(tuplesInNodeAsIndexedSeq: IndexedSeq[RoleReference],
                                      firstBorders: (Int, Int),
                                      secondBorders: (Int, Int)) {
  def totalMatchChecks = (firstBorderEnd - firstBorderStart) * (secondBorderEnd-secondBorderStart) // TODO: check if this is the correct logic here

  def secondBorderEnd: Int = secondBorders._2

  def secondBorderStart = secondBorders._1

  def firstBorderEnd: Int = firstBorders._2

  def firstBorderStart = firstBorders._1

}

object NormalPairwiseMatchingTask {

}
