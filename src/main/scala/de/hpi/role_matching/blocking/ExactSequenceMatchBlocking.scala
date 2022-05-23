package de.hpi.role_matching.blocking

import de.hpi.role_matching.cbrm.data.Roleset

import java.time.LocalDate

class ExactSequenceMatchBlocking(roleset: Roleset, trainTimeEnd:LocalDate) extends SimpleGroupBlocker{

  val groups = roleset.posToRoleLineage.values.groupBy(_.toExactValueSequence(trainTimeEnd))
  println()

}
