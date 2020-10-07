package de.hpi.dataset_versioning.db_synthesis.baseline

import de.hpi.dataset_versioning.io.IOService

object TopDownMain extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val countChangesForALlSteps = if(args.size>=3) args(2).toBoolean else true
  val toIgnore = if(args.size>=4) args(3).split(",").toSet else Set[String]()
  val topDown = new TopDown(subdomain,toIgnore)
  topDown.synthesizeDatabase(countChangesForALlSteps)
}
