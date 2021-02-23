package de.hpi.dataset_versioning.db_synthesis.graph

import de.hpi.dataset_versioning.io.IOService

object AssociationGraphEdgeCandidateGenerationMain extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  new AssociationGraphEdgeCandidateGenerator(subdomain)
    .serializeAllCandidates()

}
