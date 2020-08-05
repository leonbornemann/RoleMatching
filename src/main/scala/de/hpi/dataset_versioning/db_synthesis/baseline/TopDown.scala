package de.hpi.dataset_versioning.db_synthesis.baseline

import java.time
import java.time.LocalDate
import java.time.temporal.ChronoUnit

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.{AttributeLineage, AttributeState, ChangeCube}
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.TemporalSchema
import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.{DecomposedTemporalTable, TemporalTableDecomposer}
import de.hpi.dataset_versioning.db_synthesis.top_down_no_change.decomposition.DatasetInfo
import de.hpi.dataset_versioning.db_synthesis.top_down_no_change.decomposition.normalization.DecomposedTable

import scala.collection.mutable
import scala.concurrent.duration.Duration

class TopDown(subdomain:String) extends StrictLogging{


  def getPossibleEquivalenceClass(table: DecomposedTemporalTable) = {
    val schemaSizeHistory = mutable.TreeMap[LocalDate,Int]() ++ table.allActiveVersions
      .map(v => (v,table.schemaAt(v).size))
    val toDelete = mutable.HashSet[LocalDate]()
    var prev:Int = -1
    schemaSizeHistory.foreach{case (k,v) => {
      if(v==prev) {
        toDelete.add(k)
      }
      prev = v
    }}
    toDelete.foreach(schemaSizeHistory.remove(_))
    schemaSizeHistory
  }

  def getMergeInfo(dtt1: DecomposedTemporalTable, dtt2: DecomposedTemporalTable): TemporalTableMergeInfo = {
    ???
  }

  def getColnameContainmentBasedSchemaMapping(dtt1: DecomposedTemporalTable, dtt2: DecomposedTemporalTable) = {
    val greedyMatcher = new SuperSimpleGreedySchemaMatcher(dtt1,dtt2).getSchemaMatching()
    //dtt2 is modeled as the database table, to which we want to map the column lineages of dtt1
    //other old ideas:
//    val globalTemporalMatching1To2 = mutable.HashMap[Int,Set[Int]]()
//    val globalTemporalMatching2To1 = mutable.HashMap[Int,Set[Int]]()
//    val lineages1 = dtt1.containedAttrLineages
//    val lineages2 = dtt2.containedAttrLineages
//    val idToNameSet1 = lineages1.map(al => (al.attrId,al.nameSet)).toMap
//    val idToNameSet2 = lineages2.map(al => (al.attrId,al.nameSet)).toMap
//    val versions1 = dtt1.allActiveVersions
//    val versions2 = dtt2.allActiveVersions
//    assert(versions1==versions2)
//    for(v <- versions1){
//      val schema1 = dtt1.schemaAt(v)
//      val schema2 = dtt2.schemaAt(v)
//      assert(schema1.size==schema2.size)
//      val curMapping = mutable.HashMap[Attribute,Attribute]()
//      //first stage: use the already existing mappings
//      //second stage: mapping based on exact name match
//      val byName1 = schema1.map(a => (a.name,a)).toMap
//      val byName2 = schema2.map(a => (a.name,a)).toMap
//      val stillToMatch1 = mutable.HashSet[Attribute]() ++ schema1
//      val stillToMatch2 = mutable.HashSet[Attribute]() ++ schema2
//      byName1.foreach{case (n,a1) => {
//        if(byName2.contains(n)) {
//          val a2 = byName2(n)
//          curMapping.put(a1, a2)
//          stillToMatch1.remove(a1)
//          stillToMatch2.remove(a2)
//        }
//      }}
//      //third stage: use containment
//      stillToMatch1.foreach(a => {
//
//        //check if we have a name match anywhere in the lineage
//      })
//    }
//    assert(versions1.forall(v => {
//      val attrIds1 = dtt1.schemaAt(v).map(_.id).toSet
//      val attrIds2 = dtt1.schemaAt(v).map(_.id).toSet
//      val exactlyOneForEachAttr1 = attrIds1.forall{case (id1) => {
//        //only one partner must exist
//        val matchesIn2 = globalTemporalMatching1To2(id1)
//        val numPresent = matchesIn2.map(id2 => if(attrIds2.contains(id2)) 1 else 0).sum
//        numPresent==1
//      }}
//      //Do the same for second
//      val exactlyOneForEachAttr2 = attrIds1.forall{case (id2) => {
//        //only one partner must exist
//        val matchesIn1 = globalTemporalMatching2To1(id2)
//        val numPresent = matchesIn1.map(id1 => if(attrIds1.contains(id1)) 1 else 0).sum
//        numPresent==1
//      }}
//      exactlyOneForEachAttr1 && exactlyOneForEachAttr2
//    }))
  }

  def shouldConsiderMerge(dtt1: DecomposedTemporalTable, dtt2: DecomposedTemporalTable): Boolean = {
    val schemaMapping = getColnameContainmentBasedSchemaMapping(dtt1,dtt2)
    ???
    //TODO: continue here, maybe we should not just consider merge, but actually execute it already (?)
    //if(schemaMapping.size==dtt1.)
  }

  def synthesizeDatabase() = {
    val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
    val subdomainIds = subDomainInfo(subdomain)
      .map(_.id)
      .toIndexedSeq
    val temporallyDecomposedTablesBySchemaSizeHistory = subdomainIds.flatMap(id => {
      logger.debug(s"Loading temporally decomposed tables for $id")
      val dtts = DecomposedTemporalTable.loadAll(subdomain,id)
      dtts
    }).groupBy(getPossibleEquivalenceClass(_))
    temporallyDecomposedTablesBySchemaSizeHistory.foreach{case (schemaSizeHistory,g) => {
      logger.debug(s"Looking for potential Matches in $schemaSizeHistory")
      logger.debug(s"Group size: ${g.size}")
      val finalSynthesizedTables = mutable.HashSet[SynthesizedTemporalDatabaseTable]()
      var freeTables = mutable.HashSet[DecomposedTemporalTable]() ++ g
      for(i <- 0 until g.size){
        val dttToMerge = g(i)
        if(!freeTables.contains(dttToMerge)){
          val curSynthTable = SynthesizedTemporalDatabaseTable.initFrom(dttToMerge)
          finalSynthesizedTables.add(curSynthTable)
          val newlyMatched = mutable.HashSet[DecomposedTemporalTable]()
          freeTables.foreach(curCandidate => {
            val bestMerge = curSynthTable.getBestMerge(curCandidate)
            if(bestMerge.isDefined){
              curSynthTable.merge(curCandidate,bestMerge.get)
              newlyMatched.add(curCandidate)
            }
          })
          freeTables = freeTables.diff(newlyMatched)
        } else{
          //we skip this table because it was already merged into a synthesized table
    }}

    for(i <- 0 until mergeGraphAdjacencyList.size){
      ??? //TODO: continue here (?)
    }

    //old:
    //decompose the tables:
//    val temporalTables = subdomainIds.map(id => {
//      logger.debug(s"Loading changes for $id")
//      val changeCube = ChangeCube.load(id)
//      val temporalTable = changeCube.toTemporalTable()
//      temporalTable
//    })
//    val fieldLineageReferences = temporalTables.map(_.getFieldLineageReferences)
//    //now we need to build a compatibility index
//    val equalityCompatibilityMethod = new FieldLineageEquality()
//    val deltaTCompatibilityMethod = new DeltaTCompatibility(3)
//TODO:
    //    temporalTables.foreach(t => {
//      t.validateDiscoverdFDs()
//    })
    //TODO: this might actually be the core of the paper (?)
    //TODO: we need an index and we need to rank the connections
    //TODO: do we aggregate by column?
  }

}
