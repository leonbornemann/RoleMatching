package de.hpi.dataset_versioning.data.metadata.custom

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.TemporalSchema
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.database.table.AssociationSchema
import de.hpi.dataset_versioning.io.IOService

import java.time.LocalDate

class DatasetMetaInfoCreator(subdomain:String) extends StrictLogging{

  val associationSchemata = AssociationSchema.loadAllAssociationsInSubdomain(subdomain)
  val byID = associationSchemata.groupBy(a => (a.id.viewID))


  def tryLoadMetadata(id:String,metadataTimestamps: Set[LocalDate]) = {
    val tryFirst = metadataTimestamps.toIndexedSeq.sortBy(_.toEpochDay).head
    IOService.cacheMetadata(tryFirst)
    val md = IOService.cachedMetadata(tryFirst).getOrElse(id,null)
    val name = if(md!=null) md.resource.name else "NULL"
    val description = if(md!=null) md.resource.description.getOrElse(DatasetMetaInfo.NO_DESCRIPTION) else DatasetMetaInfo.NO_DESCRIPTION
    (name,description)
  }

  def create() = {
    byID.keySet.foreach(id => {
      logger.debug(s"Starting $id")
      val ts = TemporalSchema.load(id)
      val metadataTimestamps = IOService.getAllSimplifiedDataVersions(id).keySet
      val (name,description) = tryLoadMetadata(id,metadataTimestamps)
      val ami = byID(id).map(as => {
        val association = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(as.id)
        val cardinality = association.nrows
        val tuples = association.tupleReferences.map(_.getDataTuple.head.getValueLineage)
        assert(tuples == tuples.toSet)
        AssociationMetaInfo(as.id,as.attributeLineage.lastName,as.attributeLineage.attrId,cardinality)
      })
      DatasetMetaInfo(id,name,description,ami)
        .writeToStandardFile()
    })
  }

}
