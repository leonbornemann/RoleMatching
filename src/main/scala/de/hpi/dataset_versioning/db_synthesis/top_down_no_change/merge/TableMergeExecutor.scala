package de.hpi.dataset_versioning.db_synthesis.top_down_no_change.merge

import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.database.{SynthesizedDatabaseTable, TableUnionInfo}
import de.hpi.dataset_versioning.db_synthesis.top_down_no_change.decomposition.normalization.DecomposedTable
import de.hpi.dataset_versioning.db_synthesis.top_down_no_change.merge.grouping.ArityBasedDecomposedTableGrouper
import de.hpi.dataset_versioning.db_synthesis.top_down_no_change.merge.measures.{DummyCostMeasure, InferredChangeCountReduction, TableMergeMeasure}
import de.hpi.dataset_versioning.io.DBSynthesis_IOService
import scalax.collection.edge.LUnDiEdge
import scalax.collection.mutable.Graph
import scalax.collection.GraphPredef._
import scalax.collection.GraphEdge._

import scala.collection.mutable

/***
 * implements Problem statement 1.1
 */
class TableMergeExecutor() extends StrictLogging{

  var decomposedTableGrouper = new ArityBasedDecomposedTableGrouper()
  val graph = Graph[DecomposedTable,LUnDiEdge]()
  val costMeasure:TableMergeMeasure = new DummyCostMeasure()
  val benefitMeasure:TableMergeMeasure = new InferredChangeCountReduction()
  val tableMerger = new SchemaBasedTableMerger(benefitMeasure,costMeasure,false)

  def addEdgesForGroup(g: IndexedSeq[DecomposedTable]) = {
    logger.debug(s"processing group $g")
    for(i <- 0 until g.size){
      for(j <- i until g.size){
        val t1 = g(i)
        val t2 = g(j)
        val tableMergeResult = tableMerger.tryTableMerge(t1,t2)
        if(tableMergeResult.isDefined){
          val edge = LUnDiEdge(t1,t2)(TableMergeResult)
          graph.addAndGet(edge)
        }
      }
    }
  }

  def buildMergedTables(decomposedTableGroup: Array[DecomposedTable]) = {
    val merged = mutable.HashSet[Int]()
    val mergedTables = mutable.ArrayBuffer[SynthesizedDatabaseTable]()
    var tableId = 0
    for(i <- 0 until decomposedTableGroup.size){
      merged +=i
      val synthesizedTable = SynthesizedDatabaseTable.initFromSingleDecomposedTable(tableId.toString,LocalDate.MAX,decomposedTableGroup(i))
      mergedTables += synthesizedTable
      tableId +=1
      val unmergedCandidates = (i until decomposedTableGroup.size)
        .filter(!merged.contains(_))
      for(j <- unmergedCandidates){
        val t2 = decomposedTableGroup(j)
        val tableMergeResult = tableMerger.tryTableMerge(synthesizedTable,t2)
        if(tableMergeResult.isDefined){
          val mergeResult = tableMergeResult.get
          synthesizedTable.addUnionTable(TableUnionInfo(t2,mergeResult.columnMapping,mergeResult.cost,mergeResult.benefit))
          merged += j
        }
      }
    }
    mergedTables
  }

  def mergeTables() = {
    val decomposedTables = DBSynthesis_IOService.getDecompositionResultFiles()
      .map(f => DecomposedTable.fromJsonFile(f.getAbsolutePath))
    val bySameSchema = decomposedTables.groupBy(_.attributes.map(_.name))
    bySameSchema.foreach(g => {
      logger.debug(s"merging group ${g._1}")
      buildMergedTables(g._2)
    })
    //graph-based variant:
//    decomposedTables.foreach(t => graph.add(t))
//    val groups:Set[IndexedSeq[DecomposedTable]] = decomposedTableGrouper.getGroups(decomposedTables)
//    groups.foreach(g => {
//      addEdgesForGroup(g)
//    })
    //graph is constructed
    //TODO: now we need to find our synthesized database
  }

}
