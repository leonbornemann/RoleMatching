package de.hpi.dataset_versioning.db_synthesis.baseline

import java.io.PrintWriter
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
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

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
    schemaSizeHistory //TODO: leading and trailing zeros are allowed to match anything
  }

  def getMergeInfo(dtt1: DecomposedTemporalTable, dtt2: DecomposedTemporalTable): TemporalTableMergeInfo = {
    ???
  }

//  def getColnameContainmentBasedSchemaMapping(dtt1: DecomposedTemporalTable, dtt2: DecomposedTemporalTable) = {
//    val greedyMatcher = new SuperSimpleGreedySchemaMatcher(dtt1,dtt2).getSchemaMatching()
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
//  }

  def synthesizeDatabase() = {
    val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
    val subdomainIds = subDomainInfo(subdomain)
      .map(_.id)
      .toIndexedSeq
    val temporallyDecomposedTablesBySchemaSizeHistory = subdomainIds
      .filter(id => DBSynthesis_IOService.getDecomposedTemporalTableDir(subdomain,id).exists())
      .flatMap(id => {
        logger.debug(s"Loading temporally decomposed tables for $id")
        val dtts = DecomposedTemporalTable.loadAll(subdomain, id)
        dtts
    }).groupBy(getPossibleEquivalenceClass(_))
    logger.debug(s"Loaded ${temporallyDecomposedTablesBySchemaSizeHistory.size} decomposed temporal tables")
    val statFile = new PrintWriter("matchingStatistics.csv")
    statFile.println("dtt1,dtt2,schemaSize1,schemaSize2")
    //gather statistics:
    temporallyDecomposedTablesBySchemaSizeHistory.foreach { case (schemaSizeHistory, g) => {
      for (i <- 0 until g.size) {
        val dttToMerge1 = g(i)
        for (j <- (i+1) until g.size) {
          val dttToMerge2 = g(j)
          statFile.println(s"${dttToMerge1.compositeID},${dttToMerge2.compositeID},${dttToMerge1.containedAttrLineages.size},${dttToMerge2.containedAttrLineages.size}")
        }
      }
    }}
    statFile.close()

//    temporallyDecomposedTablesBySchemaSizeHistory.foreach { case (schemaSizeHistory, g) => {
//      logger.debug(s"Looking for potential Matches in $schemaSizeHistory")
//      logger.debug(s"Group size: ${g.size}")
//      val finalSynthesizedTables = mutable.HashSet[SynthesizedTemporalDatabaseTable]()
//      var freeTables = mutable.HashSet[DecomposedTemporalTable]() ++ g
//      for (i <- 0 until g.size) {
//        val dttToMerge = g(i)
//        if (freeTables.contains(dttToMerge)) {
//          val curSynthTable = SynthesizedTemporalDatabaseTable.initFrom(dttToMerge)
//          finalSynthesizedTables.add(curSynthTable)
//          val newlyMatched = mutable.HashSet[DecomposedTemporalTable]()
//          freeTables.remove(dttToMerge)
//          freeTables.foreach(curCandidate => {
//            val bestMerge = curSynthTable.getBestMerge(curCandidate)
//            //            if(bestMerge.isDefined){
//            //              curSynthTable.merge(curCandidate,bestMerge.get)
//            //              newlyMatched.add(curCandidate)
//            //            }
//          })
//          freeTables = freeTables.diff(newlyMatched)
//        } else {
//          //we skip this table because it was already merged into a synthesized table
//        }
//      }
//    }}
  }
}
