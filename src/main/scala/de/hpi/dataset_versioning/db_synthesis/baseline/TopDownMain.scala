package de.hpi.dataset_versioning.db_synthesis.baseline

import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.io.IOService

object TopDownMain extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  GLOBAL_CONFIG.INDEX_DEPTH = args(2).toInt
  val countChangesForALlSteps = if(args.size>=4) args(3).toBoolean else true
  val toIgnore = if(args.size>=5) args(4).split(",").toSet else Set[String]()
  val topDown = new TopDown(subdomain,toIgnore)
  topDown.synthesizeDatabase(countChangesForALlSteps)
}
