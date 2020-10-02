package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier

import scala.collection.mutable

class TemporalSchemaMapper() extends StrictLogging{

  def fromSameBCNFTable(head: DecomposedTemporalTableIdentifier, head1: DecomposedTemporalTableIdentifier): Boolean = {
    head.viewID==head1.viewID && head.bcnfID == head1.bcnfID
  }

  def getSameBCNFTableOriginMapping[A](tableA: TemporalDatabaseTableTrait[A], tableB: TemporalDatabaseTableTrait[A]) = {
    val mapping = mutable.HashMap[Set[AttributeLineage],Set[AttributeLineage]]()
    mapping.put(Set(tableA.nonKeyAttributeLineages.head),Set(tableB.nonKeyAttributeLineages.head))
    assert(tableA.primaryKey.size==tableB.primaryKey.size)
    val pkMapping = tableA.primaryKey.toIndexedSeq.sortBy(_.attrId)
      .zip(tableB.primaryKey.toIndexedSeq.sortBy(_.attrId))
      .map{case (alA,alB) => (Set(alA),Set(alB))}
    mapping ++= pkMapping
    Seq(mapping)
  }

  def getSimplePkSize1Mapping[A](tableA: TemporalDatabaseTableTrait[A], tableB: TemporalDatabaseTableTrait[A]): collection.Seq[collection.Map[Set[AttributeLineage], Set[AttributeLineage]]] ={
    val mapping = mutable.HashMap[Set[AttributeLineage],Set[AttributeLineage]]()
    mapping.put(Set(tableA.nonKeyAttributeLineages.head),Set(tableB.nonKeyAttributeLineages.head))
    mapping.put(Set(tableA.primaryKey.head),Set(tableB.primaryKey.head))
    Seq(mapping)
  }

  def getBagOfWordsOverlapScore[A](multisetA: collection.Map[A, Int], multisetB: collection.Map[A, Int]) = {
    var denom = 0
    var numerator = 0
    (multisetA.keySet ++ multisetB.keySet).foreach(k => {
      val kInA = multisetA.getOrElse(k,0)
      val kInB = multisetB.getOrElse(k,0)
      denom += Math.max(kInA,kInB)
      numerator += Math.min(kInA,kInB)
    })
    numerator / (if(denom==0) 1 else denom).toDouble
  }

  def getSimpleBagofWordsBased1To1Mapping[A](tableA: TemporalDatabaseTableTrait[A], tableB: TemporalDatabaseTableTrait[A]): collection.Seq[collection.Map[Set[AttributeLineage], Set[AttributeLineage]]] = {
    val mapping = mutable.HashMap[Set[AttributeLineage],Set[AttributeLineage]]()
    mapping.put(Set(tableA.nonKeyAttributeLineages.head),Set(tableB.nonKeyAttributeLineages.head))
    val aToBagOfWords = tableA.primaryKey
      .map(al => (al,tableA.columns.filter(_.attributeLineage==al).head.getBagOfWords()))
      .toMap
    val bToBagOfWords = tableB.primaryKey
      .map(al => (al,tableB.columns.filter(_.attributeLineage==al).head.getBagOfWords()))
      .toMap
    val attrsBUsed = mutable.HashSet[AttributeLineage]()
    for(attrA <- tableA.primaryKey){
      var curBestB:AttributeLineage = null
      var curBestBScore:Double = -1.0
      for(attrB <- tableB.primaryKey.diff(attrsBUsed)){
        val curScore = getBagOfWordsOverlapScore(aToBagOfWords(attrA),bToBagOfWords(attrB))
        if(curScore>curBestBScore){
          curBestB = attrB
          curBestBScore = curScore
        }
      }
      assert(curBestB!=null)
      attrsBUsed +=curBestB
      mapping.put(Set(attrA),Set(curBestB))
    }
    Seq(mapping)
  }

  def enumerateAllValidSchemaMappings[A](tableA: TemporalDatabaseTableTrait[A], tableB: TemporalDatabaseTableTrait[A]):collection.Seq[collection.Map[Set[AttributeLineage],Set[AttributeLineage]]] = {
    assert(tableA.nonKeyAttributeLineages.size==1)
    assert(tableA.nonKeyAttributeLineages.size==1)
    val allMappings = mutable.ArrayBuffer[collection.Map[Set[AttributeLineage],Set[AttributeLineage]]]()
    if(tableA.getUnionedTables.size==1 && tableB.getUnionedTables.size==1 && fromSameBCNFTable(tableA.getUnionedTables.head,tableB.getUnionedTables.head)){
      //we can do an easy mapping, because we know the mapping (by ID)
      return getSameBCNFTableOriginMapping(tableA,tableB)
    } else if(tableA.primaryKey.size == 1 && tableB.primaryKey.size==1) {
      return getSimplePkSize1Mapping(tableA,tableB)
    } else if(tableA.primaryKey.size == tableB.primaryKey.size) {
      //"easy case" we create a bipartite matching according to the highest scores given bag of words
      //create bad of words:
      return getSimpleBagofWordsBased1To1Mapping(tableA,tableB)

    } else{
      ??? //untested - we should not consider this for now
      val colCombinationEnumerator = new LatticeBasedInterTableColumnMergeEnumerator()
      //TODO: Test this!
      val allValidAttributeCombinationsA = colCombinationEnumerator.enumerateAll(tableA.primaryKey.toIndexedSeq)
      val allValidAttributeCombinationsB = colCombinationEnumerator.enumerateAll(tableB.primaryKey.toIndexedSeq)
      //TODO:
      if(allValidAttributeCombinationsA.size==1 && allValidAttributeCombinationsB.contains(tableB.primaryKey) ||
        allValidAttributeCombinationsB.size==1 && allValidAttributeCombinationsA.contains(tableA.primaryKey)){
        val mapping = mutable.HashMap[Set[AttributeLineage],Set[AttributeLineage]]()
        mapping.put(Set(tableA.nonKeyAttributeLineages.head),Set(tableB.nonKeyAttributeLineages.head))
        mapping.put(Set() ++ tableA.primaryKey,Set() ++ tableB.primaryKey)
        allMappings.addOne(mapping)
      } else {
        ??? //TODO: implement this in a reasonable way
      }
    }
    allMappings
  }
}
object TemporalSchemaMapper extends StrictLogging{
  logger.debug("TemporalSchemaMapper currently only supports 1-to-1 matchings for key attribute lineages of size >1 and returns empty mappings for cases where the primary keys do not have the same size")
  logger.debug("For this, it uses a greedy algorithm - might not be optimal, but should be in most cases")

}
