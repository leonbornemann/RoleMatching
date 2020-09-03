package de.hpi.dataset_versioning.db_synthesis.baseline

import de.hpi.dataset_versioning.io.IOService

object TopDownIDListMain extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val ids = args.slice(2,args.size)
  val topDown = new TopDown(subdomain)
  topDown.synthesizeDatabase(ids.toIndexedSeq,true)
}
