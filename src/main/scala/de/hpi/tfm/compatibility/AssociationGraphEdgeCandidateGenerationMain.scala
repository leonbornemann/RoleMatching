package de.hpi.tfm.compatibility

import de.hpi.tfm.io.IOService

import java.time.LocalDate

object AssociationGraphEdgeCandidateGenerationMain extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val minEvidence = args(2).toInt
  val timeRangeStart = LocalDate.parse(args(3))
  val timeRangeEnd = LocalDate.parse(args(4))
  val graphConfig = GraphConfig(minEvidence,timeRangeStart,timeRangeEnd)
  new AssociationGraphEdgeCandidateGenerator(subdomain,graphConfig)
    .serializeAllCandidates()

}
