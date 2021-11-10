package de.hpi.role_matching.cbrm.evidence_based_weighting.isf

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.data.{RoleLineageWithID, Roleset}

import java.io.File

object ISFMapExtraction extends App with StrictLogging {
  logger.debug(s"called with ${args.toIndexedSeq}")
  val datasource = args(0)
  GLOBAL_CONFIG.setSettingsForDataSource(datasource)
  val inputRoleFile = args(1)
  val resultFile = new File(args(2))
  val roleset = Roleset.fromJsonFile(inputRoleFile)
  val hist = RoleLineageWithID.getTransitionHistogramForTFIDFFromVertices(roleset.positionToRoleLineage.values.toIndexedSeq, GLOBAL_CONFIG.granularityInDays)
  ISFMapStorage(hist.toIndexedSeq).toJsonFile(resultFile)
}
