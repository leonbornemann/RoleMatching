package de.hpi.dataset_versioning.db_synthesis.preparation.simplifiedExport

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.{AttributeLineage, SurrogateAttributeLineage}
import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.{SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedTemporalRow}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.database.GlobalSurrogateRegistry
import de.hpi.dataset_versioning.db_synthesis.database.table.AssociationSchema
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

class SimplifiedInputExporter(subdomain: String, id: String) extends StrictLogging{

  var numAssociationsWithChangesAfterStandardTimeEnd = 0

  def exportAll() = {
    logger.debug(s"processing $id")
    val tt = TemporalTable.load(id)
    tt.attributes.zipWithIndex.foreach{case (al,i) => {
      val attrID = tt.attributes(i).attrId
      val dttID = DecomposedTemporalTableIdentifier(subdomain, id, 0, Some(i),Some(attrID))
      val surrogateID = GlobalSurrogateRegistry.getNextFreeSurrogateID
      val surrogateKeyAttribute = SurrogateAttributeLineage(surrogateID, i)
      //create dictionary from entity ids to surrogate key in association:
      val vlToSurrogateKey = scala.collection.mutable.HashMap[ValueLineage,Int]()
      val entityIDToSurrogateKey = scala.collection.mutable.HashMap[Long,Int]()
      var curSurrogateCounter = 0
      tt.rows.foreach(tr => {
        if(vlToSurrogateKey.contains(tr.fields(i))){
          val surrogate = vlToSurrogateKey(tr.fields(i))
          entityIDToSurrogateKey.put(tr.entityID,surrogate)
          //nothing to add to the association
        } else {
          vlToSurrogateKey.put(tr.fields(i),curSurrogateCounter)
          entityIDToSurrogateKey.put(tr.entityID,curSurrogateCounter)
          curSurrogateCounter +=1
        }
      })
      val associationFullTimeRange: SurrogateBasedSynthesizedTemporalDatabaseTableAssociation = buildAssociation(al, dttID, surrogateKeyAttribute, vlToSurrogateKey)
      val associationLimitedTimeRange = buildAssociation(al,dttID,surrogateKeyAttribute,vlToSurrogateKey,true)
      if(vlToSurrogateKey.keySet.map(_.lineage.keySet).flatten.exists(_.isAfter(IOService.STANDARD_TIME_FRAME_END))){
        numAssociationsWithChangesAfterStandardTimeEnd +=1
      }
      writeAssociationSchemaFile(al, dttID, surrogateKeyAttribute)
      associationLimitedTimeRange.writeToStandardOptimizationInputFile
      associationLimitedTimeRange.toSketch.writeToStandardOptimizationInputFile()
      associationFullTimeRange.writeToFullTimeRangeFile()
      writeFactTable(dttID, vlToSurrogateKey, entityIDToSurrogateKey)
    }}
    val allTImstamps = tt.rows.flatMap(r =>
      r.fields.flatMap(_.lineage.keySet).toSet).toSet
    val ttContainsEvaluationChanges = allTImstamps.exists(_.isAfter(IOService.STANDARD_TIME_FRAME_END))
    if(ttContainsEvaluationChanges && numAssociationsWithChangesAfterStandardTimeEnd ==0){
      println(s"changes after standard time are not kept in associations in $id")
      assert(false)
    }
  }

  private def writeAssociationSchemaFile(al: AttributeLineage, dttID: DecomposedTemporalTableIdentifier, surrogateKeyAttribute: SurrogateAttributeLineage) = {
    val schema = new AssociationSchema(dttID, surrogateKeyAttribute, al)
    schema.writeToStandardFile()
  }

  private def buildAssociation(al: AttributeLineage,
                               dttID: DecomposedTemporalTableIdentifier,
                               surrogateKeyAttribute: SurrogateAttributeLineage,
                               vlToSurrogateKey: mutable.HashMap[ValueLineage, Int],
                               shortenToStandardTimeRange:Boolean=false) = {
    val newRows = collection.mutable.ArrayBuffer() ++ vlToSurrogateKey
      .toIndexedSeq
      .sortBy(_._2)
      .map { case (vl, surrogateKey) => {
        val finalVL = if(shortenToStandardTimeRange)
            ValueLineage(vl.lineage.filter(!_._1.isAfter(IOService.STANDARD_TIME_FRAME_END)))
          else
            vl
        new SurrogateBasedTemporalRow(IndexedSeq(surrogateKey), finalVL, IndexedSeq())
      }}
      .filter(!_.valueLineage.lineage.isEmpty)
    val association = new SurrogateBasedSynthesizedTemporalDatabaseTableAssociation(dttID.compositeID,
      mutable.HashSet(),
      mutable.HashSet(dttID),
      IndexedSeq(surrogateKeyAttribute),
      al,
      IndexedSeq(),
      newRows
    )
    association
  }

  private def writeFactTable(dttID: DecomposedTemporalTableIdentifier, vlToSurrogateKey: mutable.HashMap[ValueLineage, Int], entityIDToSurrogateKey: mutable.HashMap[Long, Int]) = {
    val surrogateKeyToVL = vlToSurrogateKey
      .map(t => (t._2, t._1))
      .toIndexedSeq
    val factTableRows = entityIDToSurrogateKey
      .map { case (e, sk) => {
        FactTableRow(e, sk)
      }
      }.toIndexedSeq
    val factLookupTable = new FactLookupTable(dttID, factTableRows, surrogateKeyToVL)
    factLookupTable.writeToStandardFile()
  }
}
