package de.hpi.dataset_versioning.db_synthesis.baseline.decomposition

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.AttributeLineage
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.AttributeLineageWithHashMap
import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

case class DecomposedTemporalTable(subdomain:String,
                                   originalID: String,
                                   id:Int,
                                   containedAttrLineages: collection.IndexedSeq[AttributeLineage],
                                   originalFDLHS: collection.Set[AttributeLineage],
                                   primaryKeyByVersion: Map[LocalDate,collection.Set[Attribute]],
                                   furtherDecompID:Option[Int] = None) {

  def furtherDecomposeToAssociations = {
    //assert that every attribute lineage is either always in the primary key or not:
    val byID = containedAttrLineages.map(al => (al.attrId,al)).toMap
    val pkAttrIds = primaryKeyByVersion.values.flatMap(_.map(_.id)).toSet
    val pkAttrIsAlwaysPKIFItExists =  primaryKeyByVersion.keySet.forall(v => {
      pkAttrIds.filter(alID => byID(alID).valueAt(v)._2.exists)
        .forall(alID => primaryKeyByVersion(v).exists(a => a.id==alID))
    })
    if(!pkAttrIsAlwaysPKIFItExists){
      println()
    }
    assert(pkAttrIsAlwaysPKIFItExists)
    val pkAttributeLineages = containedAttrLineages.filter(al => pkAttrIds.contains(al.attrId))
    val nonPkAttrs = containedAttrLineages.filter(!pkAttributeLineages.contains(_))
    nonPkAttrs.zipWithIndex.map{case (rhs,i) => new DecomposedTemporalTable(subdomain,originalID,id,pkAttributeLineages ++ IndexedSeq(rhs),originalFDLHS,primaryKeyByVersion,Some(i))}
  }

  def compositeID: String = originalID + "." + id + (if(furtherDecompID.isDefined) "_" + furtherDecompID.get.toString else "")

  def schemaAt(v: LocalDate) = containedAttrLineages
    .withFilter(_.valueAt(v)._2.exists)
    .map(_.valueAt(v)._2.attr.get)
    .sortBy(_.position.get)


  def writeToStandardFile() = {
    val file = DBSynthesis_IOService.getDecomposedTemporalTableFile(subdomain,originalID,id)
    val helper = DecomposedTemporalTableHelper(subdomain,originalID,id,
      containedAttrLineages.map(AttributeLineageWithHashMap.from(_)),
      originalFDLHS.map(AttributeLineageWithHashMap.from(_)),
      primaryKeyByVersion)
    helper.toJsonFile(file)
  }

  def allActiveVersions = containedAttrLineages.flatMap(_.lineage.keySet).toSet

}

object DecomposedTemporalTable {

  def loadAll(subdomain: String, originalID: String) = {
    val dir = DBSynthesis_IOService.getDecomposedTemporalTableDir(subdomain,originalID)
    val ids = dir.listFiles().map(_.getName.split("\\.")(0).toInt)
    ids.map(id => load(subdomain,originalID,id))
  }


  def load(subdomain:String,originalID:String,id:Int) = {
    val file = DBSynthesis_IOService.getDecomposedTemporalTableFile(subdomain,originalID,id)
    val helper = DecomposedTemporalTableHelper.fromJsonFile(file.getAbsolutePath)
    helper.toDecomposedTemporalTable
  }

}
