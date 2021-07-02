package de.hpi.socrata.metadata.custom

import com.typesafe.scalalogging.StrictLogging
import de.hpi.socrata.io.Socrata_IOService
import de.hpi.socrata.metadata.custom
import de.hpi.socrata.metadata.custom.schemaHistory.TemporalSchema
import de.hpi.socrata.tfmp_input.association.AssociationSchema
import de.hpi.socrata.tfmp_input.table.nonSketch.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation

import java.time.LocalDate

class DatasetMetaInfoCreator(subdomain:String) extends StrictLogging{

  val associationSchemata = AssociationSchema.loadAllAssociationsInSubdomain(subdomain)
  val byID = associationSchemata.groupBy(a => (a.id.viewID))


  def tryLoadMetadata(id:String,metadataTimestamps: Set[LocalDate]) = {
    val tryFirst = metadataTimestamps.toIndexedSeq.sortBy(_.toEpochDay).head
    Socrata_IOService.cacheMetadata(tryFirst)
    val md = Socrata_IOService.cachedMetadata(tryFirst).getOrElse(id,null)
    val name = if(md!=null) md.resource.name else "NULL"
    val description = if(md!=null) md.resource.description.getOrElse(DatasetMetaInfo.NO_DESCRIPTION) else DatasetMetaInfo.NO_DESCRIPTION
    (name,description)
  }

  def create() = {
    byID.keySet.foreach(id => {
      logger.debug(s"Starting $id")
      val ts = TemporalSchema.load(id)
      val metadataTimestamps = Socrata_IOService.getAllSimplifiedDataVersions(id).keySet
      val (name,description) = tryLoadMetadata(id,metadataTimestamps)
      val ami = byID(id).map(as => {
        val association = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(as.id)
        val cardinality = association.nrows
        val tuples = association.tupleReferences.map(_.getDataTuple.head.getValueLineage)
        //assert(tuples.size == tuples.toSet.size)
        AssociationMetaInfo(as.id,as.attributeLineage.lastName,as.attributeLineage.attrId,cardinality)
      })
      custom.DatasetMetaInfo(id,name,description,ami)
        .writeToStandardFile()
    })
  }

}
