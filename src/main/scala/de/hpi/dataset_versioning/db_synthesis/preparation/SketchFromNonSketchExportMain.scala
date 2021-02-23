package de.hpi.dataset_versioning.db_synthesis.preparation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.database.table.AssociationSchema
import de.hpi.dataset_versioning.io.IOService

/***
 * Probably a one-shot because of RowDeletes becoming wildcards
 */
object SketchFromNonSketchExportMain extends App with StrictLogging{
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val schemata = AssociationSchema.loadAllAssociationsInSubdomain(subdomain)
  schemata.foreach(s => {
    logger.debug(s"Processing ${s.id}")
    val synthTable = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(s.id)
    synthTable.toSketch.writeToStandardOptimizationInputFile()
  })

}
