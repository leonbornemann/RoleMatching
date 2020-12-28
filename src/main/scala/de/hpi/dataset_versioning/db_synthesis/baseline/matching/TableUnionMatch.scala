package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.AbstractSurrogateBasedTemporalRow
import de.hpi.dataset_versioning.db_synthesis.sketches.field.AbstractTemporalField

import scala.collection.mutable

@SerialVersionUID(3L)
class TableUnionMatch[A](val firstMatchPartner:TemporalDatabaseTableTrait[A],
                      val secondMatchPartner:TemporalDatabaseTableTrait[A],
                      val schemaMapping:Option[collection.Map[Set[AttributeLineage], Set[AttributeLineage]]],
                      val evidence:Int,
                      val changeBenefit:(Int,Int),
                      val isHeuristic:Boolean,
                      val tupleMapping:Option[TupleSetMatching[A]]) extends Serializable{

  override def toString: String = s"(${firstMatchPartner},$secondMatchPartner){$evidence} --> [${changeBenefit._1},${changeBenefit._2}])"

  def buildUnionedTable = {
    val unionID = firstMatchPartner.getID + "_UNION_" + secondMatchPartner.getID
    val newNonKEyAttrLineage:AttributeLineage = firstMatchPartner.getNonKeyAttribute.union(firstMatchPartner.getNonKeyAttribute,secondMatchPartner.getNonKeyAttribute.attrId)
    var curSurrogateKeyCounter = 0
    val matchingToNewSurrogateKey = mutable.HashMap[General_Many_To_Many_TupleMatching[A],Int]()
    val newRows = mutable.ArrayBuffer[AbstractSurrogateBasedTemporalRow[A]]()
    tupleMapping.get.matchedTuples.foreach(matching => {
      matchingToNewSurrogateKey.put(matching,curSurrogateKeyCounter)
      val merged = AbstractTemporalField.mergeAll(matching.tupleReferences)
      curSurrogateKeyCounter +=1
      newRows += firstMatchPartner.buildNewRow(curSurrogateKeyCounter,merged)
    })
    assert(firstMatchPartner.getForeignKeys.isEmpty && secondMatchPartner.getForeignKeys.isEmpty)
    val unionedTable = firstMatchPartner.createNewTable(unionID,
        mutable.HashSet(firstMatchPartner.getUniqueSynthTableID,secondMatchPartner.getUniqueSynthTableID),
        mutable.HashSet() ++ firstMatchPartner.getUnionedOriginalTables.union(secondMatchPartner.getUnionedOriginalTables),
        firstMatchPartner.getKey,
        newNonKEyAttrLineage,
        newRows
      )
    (unionedTable,matchingToNewSurrogateKey)
  }

  def getNonOverlappingElement[A](other: TableUnionMatch[A]) = {
    assert(hasParterOverlap(other))
    assert(!(firstMatchPartner == other.firstMatchPartner &&
      secondMatchPartner == other.secondMatchPartner ||
      secondMatchPartner == other.firstMatchPartner &&
      firstMatchPartner == other.secondMatchPartner))
    if(firstMatchPartner == other.firstMatchPartner || firstMatchPartner == other.secondMatchPartner)
      secondMatchPartner
    else
      firstMatchPartner
  }


  def hasParterOverlap[A](other: TableUnionMatch[A]): Boolean = {
    firstMatchPartner == other.firstMatchPartner ||
      firstMatchPartner == other.secondMatchPartner ||
      secondMatchPartner == other.firstMatchPartner ||
      secondMatchPartner == other.secondMatchPartner
  }

}
