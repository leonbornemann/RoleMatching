package de.hpi.dataset_versioning.db_synthesis.preparation.simplifiedExport

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.SurrogateAttributeLineage
import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.{SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedTemporalRow}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.database.GlobalSurrogateRegistry
import de.hpi.dataset_versioning.db_synthesis.database.table.AssociationSchema

import scala.collection.mutable

class SimplifiedInputExporter(subdomain: String, id: String) {

  def exportAll() = {
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
      val newRows = collection.mutable.ArrayBuffer() ++ vlToSurrogateKey
        .toIndexedSeq
        .sortBy(_._2)
        .map{case (vl,surrogateKey) => new SurrogateBasedTemporalRow(IndexedSeq(surrogateKey),vl,IndexedSeq())}
      val association = new SurrogateBasedSynthesizedTemporalDatabaseTableAssociation(dttID.compositeID,
        mutable.HashSet(),
        mutable.HashSet(dttID),
        IndexedSeq(surrogateKeyAttribute),
        al,
        IndexedSeq(),
        newRows
      )
      assert(false) //TODO: shorten the data first (timestamp-wise)?
      val schema = new AssociationSchema(dttID,surrogateKeyAttribute,al)
      schema.writeToStandardFile()
      association.writeToStandardOptimizationInputFile
      association.toSketch.writeToStandardOptimizationInputFile()
      val surrogateKeyToVL = vlToSurrogateKey
        .map(t => (t._2,t._1))
        .toIndexedSeq
      val factTableRows = entityIDToSurrogateKey
        .map{case (e,sk) => {
          FactTableRow(e,sk)
        }}.toIndexedSeq
      val factLookupTable = new FactLookupTable(dttID,factTableRows,surrogateKeyToVL)
      factLookupTable.writeToStandardFile()

    }}
  }

}
