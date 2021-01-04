package de.hpi.dataset_versioning.db_synthesis.baseline.decomposition

import de.hpi.dataset_versioning.db_synthesis.baseline.SummaryChangeCounting.{subDomainInfo, subdomain}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableCreationMain.args
import de.hpi.dataset_versioning.io.IOService

object DecompositionCompletionMain extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val ids = subDomainInfo(subdomain)
    .map(_.id)
    .toIndexedSeq
  val decompositionCompleter = new DecompositionCompleter(subdomain)
  ids.foreach(decompositionCompleter.completeDecomposition(_))

}
