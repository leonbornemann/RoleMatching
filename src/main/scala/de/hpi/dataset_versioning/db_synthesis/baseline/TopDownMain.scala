package de.hpi.dataset_versioning.db_synthesis.baseline

import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.{SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.{AssociationGraphEdge, DataBasedMatchCalculator}
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

import java.time.LocalDate
import scala.io.Source

object TopDownMain extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  GLOBAL_CONFIG.INDEX_DEPTH = args(2).toInt
  val countChangesForALlSteps = args(3).toBoolean
  val loadFilteredAssociationsOnly = args(4).toBoolean
  val toIgnore = if(args.size>=6) args(5).split(",").toSet else Set[String]()
  val topDown = new TopDown(subdomain,loadFilteredAssociationsOnly,toIgnore)
  topDown.synthesizeDatabase(countChangesForALlSteps)


}
