package de.hpi.dataset_versioning.db_synthesis.preparation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.surrogate_based.SurrogateBasedDecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.{DataBasedMatchCalculator, FieldLineageMatchGraph}
import de.hpi.dataset_versioning.db_synthesis.database.table.{AssociationSchema, BCNFTableSchema}
import de.hpi.dataset_versioning.db_synthesis.preparation.OptimizationInputExportMain.{logger, subdomain}
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

class OptimizationInputExporter(subdomain:String) extends StrictLogging{

  def exportForID(id: String) = {
    logger.debug(s"Exporting $id")
    val tt = TemporalTable.load(id)
    val bcnfTables = BCNFTableSchema.loadAllBCNFTableSchemata(subdomain, id)
    val bcnfByID = bcnfTables.map(a => ((a.id.subdomain, a.id.viewID, a.id.bcnfID), a)).toMap
    //whole tables:
    val associations = if (DBSynthesis_IOService.associationSchemataExist(subdomain, id)) AssociationSchema.loadAllAssociations(subdomain, id) else Array[AssociationSchema]()
    val allSurrogates = bcnfTables.flatMap(_.surrogateKey)
      .groupBy(_.surrogateID)
      .map { case (k, v) => (k, v.head) }
    //integrity check: all references must also be used as keys:
    assert(bcnfTables.flatMap(_.foreignSurrogateKeysToReferencedBCNFTables.toMap.keySet).forall(s => allSurrogates.contains(s.surrogateID)))
    tt.addSurrogates(allSurrogates.values.toSet ++ associations.map(_.surrogateKey).toSet)
    //for change counting purposes we write the projections of bcnf tables containing data:
    val dtts = SurrogateBasedDecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain, id)
    dtts.foreach(dtt => {
      val projection = tt.project(dtt)
      projection.projection.writeTOBCNFTemporalTableFile
    })
    val byBcnf = associations.groupBy(a => (a.id.subdomain, a.id.viewID, a.id.bcnfID))
      .map { case (k, v) => (bcnfByID(k), v) }
    byBcnf.foreach { case (bcnf, associations) => {
      val (bcnfReferenceTable, synthTableAssocitations) = tt.project(bcnf, associations)
      bcnfReferenceTable.writeToStandardOptimizationInputFile
      synthTableAssocitations.foreach(t => {
        t.writeToStandardOptimizationInputFile
        t.toSketch.writeToStandardOptimizationInputFile()
      })
    }
    }
    //additionally serialize bcnf tables that have only keys and foreign keys (and no associations, so they don't show up in byBcnf):
    bcnfTables.toSet.diff(byBcnf.keySet).foreach(bcnf => {
      val (bcnfReferenceTable, _) = tt.project(bcnf, Seq())
      bcnfReferenceTable.writeToStandardOptimizationInputFile
    })
    //for every association: also export self-matchings:
    byBcnf.values
      .flatten
      .foreach(a => {
        val table = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(a.id)
        val tuples = table.tupleReferences
        val graph = new FieldLineageMatchGraph[Any](tuples)
        InternalFieldLineageEdges(a.id,graph.edges.toIndexedSeq).writeToStandardFile()
      })
  }

}
