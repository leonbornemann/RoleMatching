package de.hpi.dataset_versioning.db_synthesis.bottom_up

import java.io.PrintWriter
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.data.change.{Change, ChangeCube}
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.SchemaHistory
import de.hpi.dataset_versioning.db_synthesis.top_down.decomposition.DatasetInfo
import de.hpi.dataset_versioning.db_synthesis.top_down.main.ChangeExplorationMain.{logger, subdomain}

import scala.collection.mutable
import scala.io.Source

class BottomUpDBSynthesis(subdomain: String) extends StrictLogging{

  def buildFieldLineages(changeCube: ChangeCube) = {
    changeCube.allChanges.groupBy(c => (c.e,c.pID))
      .map{case ((e,p),changeList) => FieldLineage.fromCangeList(Field(changeCube.datasetID,e,p),changeList)}
  }

  def getMinimalEdgeCount(g: IndexedSeq[FieldLineage]): Int = {
    val byTableAndAttr = g.groupBy(l => (l.field.tableID,l.field.entityID))
    byTableAndAttr.size
  }

  def synthesize() = {
    val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
    val subdomainIds = subDomainInfo(subdomain)
      .map(_.id)
      .toIndexedSeq
    var viewFieldCount = 0
    val fieldLineages = subdomainIds.flatMap(id => {
      logger.debug(s"Loading changes for $id")
      val changeCube = ChangeCube.load(id)
      val fieldLineages = buildFieldLineages(changeCube)
      viewFieldCount += fieldLineages.size
      fieldLineages
    })
    //group by value set of field lineage - then refine the groups by doing exact comparison
    val potentialGroups = fieldLineages.groupBy(l => l.valueSet)
      .map(_._2)
    val finalGroups = potentialGroups.flatMap(group => FieldLineage.partitionToEquivalenceClasses(group))
    //TODO: finalGroups still contains many instances of groups where there is the same column of the same table multiple times
    logger.debug(s"Found ${finalGroups.map(g => getMinimalEdgeCount(g)).sum} database fields (best possible synthesized Database). Number of view fields: $viewFieldCount")
    //val a = finalGroups.filter(g => g.map(_.field.tableID).toSet.size>1 && g.head.lineage.size>1)
    val schemaInfo = SchemaHistory.loadAll()
    val pr = new PrintWriter("fieldLineageEquality.json")
    finalGroups.foreach(l => {
      val datasetColumns = l.map(fl => (fl.field.tableID,fl.field.attributeID)).toSet
      val cols = datasetColumns.map{case (id,cID) => {
        val schema = schemaInfo(id)
        val column = schema.superSchema.filter(_.id==cID).head
        DatasetColumn(id,column.name,column.humanReadableName.getOrElse(null))
      }}
      val info = FieldLineageOccurrenceInfo(l.head.lineage.toIndexedSeq,cols,l.head.field)
      info.appendToWriter(pr,false,true,false)
    })
    pr.close()
  }

}
