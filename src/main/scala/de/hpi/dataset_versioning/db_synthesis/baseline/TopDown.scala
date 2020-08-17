package de.hpi.dataset_versioning.db_synthesis.baseline

import java.io.PrintWriter
import java.time
import java.time.LocalDate
import java.time.temporal.ChronoUnit

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.ChangeCube
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
    schemaSizeHistory //TODO: leading and trailing zeros are allowed to match anything, also we are not sure yet if we might want to allow projections after all
  }

  def getMergeInfo(dtt1: DecomposedTemporalTable, dtt2: DecomposedTemporalTable): TemporalTableMergeInfo = {
    ???
  }

  def synthesizeDatabase() = {
    val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
    val subdomainIds = subDomainInfo(subdomain)
      .map(_.id)
      .toIndexedSeq
    val temporallyDecomposedTables = subdomainIds
      .filter(id => DBSynthesis_IOService.getDecomposedTemporalTableDir(subdomain,id).exists())
      .flatMap(id => {
        logger.debug(s"Loading temporally decomposed tables for $id")
        val dtts = DecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain, id)
        dtts.flatMap(dtt => dtt.furtherDecomposeToAssociations)
      })
    val topDownOptimizer = new TopDownOptimizer(temporallyDecomposedTables)
    topDownOptimizer.optimize()
    //TODO: how to keep track of queries (?) --> not sure yet how to do that
    println(temporallyDecomposedTables.size)
    val temporallyDecomposedTablesBySchemaSizeHistory =  temporallyDecomposedTables.groupBy(getPossibleEquivalenceClass(_))
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

    temporallyDecomposedTablesBySchemaSizeHistory.foreach { case (schemaSizeHistory, g) => {
      logger.debug(s"Looking for potential Matches in $schemaSizeHistory")
      logger.debug(s"Group size: ${g.size}")
      val finalSynthesizedTables = mutable.HashSet[SynthesizedTemporalDatabaseTable]()
      var freeTables = mutable.HashSet[DecomposedTemporalTable]() ++ g
      for (i <- 0 until g.size) {
        val dttToMerge = g(i)
        if (freeTables.contains(dttToMerge)) {
          val curSynthTable = SynthesizedTemporalDatabaseTable.initFrom(dttToMerge)
          finalSynthesizedTables.add(curSynthTable)
          val newlyMatched = mutable.HashSet[DecomposedTemporalTable]()
          freeTables.remove(dttToMerge)
          freeTables.foreach(curCandidate => {
            val bestMerge = curSynthTable.getBestMergeMapping(curCandidate)
            assert(false) //TODO: comment in the code below
//            if(bestMerge.isDefined){
//              curSynthTable.merge(curCandidate,bestMerge.get)
//              newlyMatched.add(curCandidate)
//            }
          })
          freeTables = freeTables.diff(newlyMatched)
        } else {
          //we skip this table because it was already merged into a synthesized table
        }
      }
    }}
  }
}
