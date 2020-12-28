package de.hpi.dataset_versioning.db_synthesis.baseline

import de.hpi.dataset_versioning.io.IOService

object TopDownIDListMain extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val loadFilteredAssociationsOnly = args(2).toBoolean
  val ids = args.slice(3,args.size)
  val topDown = new TopDown(subdomain,loadFilteredAssociationsOnly)
  topDown.synthesizeDatabase(ids.toIndexedSeq,true)
}
