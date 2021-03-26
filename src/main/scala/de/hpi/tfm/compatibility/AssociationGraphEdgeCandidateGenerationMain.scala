package de.hpi.tfm.compatibility

import de.hpi.tfm.io.IOService

object AssociationGraphEdgeCandidateGenerationMain extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  new AssociationGraphEdgeCandidateGenerator(subdomain)
    .serializeAllCandidates()

}
