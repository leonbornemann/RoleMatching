package de.hpi.dataset_versioning.db_synthesis.baseline.decomposition

import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.{AttributeLineage, AttributeState, SurrogateAttributeLineage}
import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.natural_key_based.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.surrogate_based.SurrogateBasedDecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.database.GlobalSurrogateRegistry

import scala.collection.mutable

class SurrogateBasedTemporalTableDecomposer {


  def createSurrogateBasedDtts(dtts: IndexedSeq[DecomposedTemporalTable]) = {
    val allAttrIds = dtts.flatMap(_.containedAttrLineages.map(_.attrId)).toSet
    val idToReferredTables = allAttrIds.map( id => {
      (id,dtts.filter(_.primaryKey.exists(_.attrId == id)))
    }).toMap
    val idToLineages = dtts.flatMap(_.containedAttrLineages)
      .groupBy(_.attrId)
    //assert equality:
    //idToLineages.foreach(t => assert((0 until t._2.size-1).forall(i => t._2(i).lineage==t._2(i+1).lineage)))
    val idToAttribute = idToLineages.map{case (id,group) => (id,group.head)}
    val pkIds = dtts
      .flatMap(_.primaryKey.map(_.attrId)).toSet
    val naturalKeyIDToSurrogateID = pkIds.map(i => {
      val attr = idToAttribute(i)
      val toReturn = (i,(GlobalSurrogateRegistry.getNextFreeSurrogateID,attr))
      (toReturn)
    }).toMap
    val naturalKeyOccurredInTable = mutable.HashMap() ++ naturalKeyIDToSurrogateID.keySet.map(id => (id,false)).toMap
    val surrogateBasedTables = dtts.map(dtt => {
      val newNonKeys = mutable.ArrayBuffer[AttributeLineage]()
      val oldReferences = mutable.ArrayBuffer[(AttributeLineage,collection.IndexedSeq[DecomposedTemporalTableIdentifier])]()
      dtt.containedAttrLineages.foreach(al => {
        val otherReferencedTables = idToReferredTables(al.attrId).filter(_!=dtt)
        if(otherReferencedTables.isEmpty || dtt.primaryKey.contains(al) && !naturalKeyOccurredInTable(al.attrId)) {
          newNonKeys += al
          if(dtt.primaryKey.contains(al))
            naturalKeyOccurredInTable(al.attrId) = true
        }
        else {
          val toAdd = (al,otherReferencedTables.map(_.id))
          oldReferences.addOne(toAdd)
        }
      })
      //dtt.containedAttrLineages.partition(al => idToReferredTables(al.attrId).filter(_.))
      val newKey = dtt.primaryKey.toIndexedSeq
        .map(pk => {
          createSurrogateAttribute(naturalKeyIDToSurrogateID, pk)
        })
      val newReferences = oldReferences.map{case (fk,referredTables) => {
        (createSurrogateAttribute(naturalKeyIDToSurrogateID, fk),referredTables)
      }}.toIndexedSeq.sortBy(_._1.surrogateID)
      new SurrogateBasedDecomposedTemporalTable(dtt.id,newKey,newNonKeys,newReferences)
    })
    assert(surrogateBasedTables.flatMap(_.attributes.map(_.attrId)).size == dtts.flatMap(_.containedAttrLineages).map(_.attrId).toSet.size)
    surrogateBasedTables.foreach(sbdtt => {
      sbdtt.writeToStandardFile()
      val (bcnfReferenceTable,associations) = sbdtt.furtherDecomposeToAssociations
      associations.foreach(sbdta => sbdta.writeToStandardFile())
      //sbdtt.getReferenceSkeleton().writeToStandardFile()
      bcnfReferenceTable.writeToStandardFile()
    })
    //new AttributeLineage(curSurrogateKeyID,mutable.TreeMap[LocalDate,AttributeState](initialInsert -> ),true)
  }

  private def createSurrogateAttribute(naturalKeyIDToSurrogateID: Map[Int, (Int, AttributeLineage)], pk: AttributeLineage) = {
    val (surrogateID, original) = naturalKeyIDToSurrogateID(pk.attrId)
    val newName = original.lastName + s"_SURROGATE_ID[$surrogateID]"
    val newAttributeState = new AttributeState(Some(Attribute(original.lastName + s"_SURROGATE_ID[$surrogateID]", surrogateID, None, None))) //this will always be position 0 now
    new SurrogateAttributeLineage(surrogateID, original.attrId)
  }

}
