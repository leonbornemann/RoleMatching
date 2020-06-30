package de.hpi.dataset_versioning.db_synthesis.top_down.merge

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.top_down.decomposition.normalization.DecomposedTable
import de.hpi.dataset_versioning.db_synthesis.top_down.merge.grouping.ArityBasedDecomposedTableGrouper
import de.hpi.dataset_versioning.db_synthesis.top_down.merge.measures.{DummyCostMeasure, InferredChangeCountReduction, TableMergeMeasure}
import de.hpi.dataset_versioning.io.DBSynthesis_IOService
import scalax.collection.edge.LUnDiEdge
import scalax.collection.mutable.Graph
import scalax.collection.GraphPredef._, scalax.collection.GraphEdge._

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

  def mergeTables() = {
    val decomposedTables = DBSynthesis_IOService.getDecompositionResultFiles()
      .map(f => DecomposedTable.fromJsonFile(f.getAbsolutePath))
    decomposedTables.foreach(t => graph.add(t))
    val groups:Set[IndexedSeq[DecomposedTable]] = decomposedTableGrouper.getGroups(decomposedTables)
    groups.foreach(g => {
      addEdgesForGroup(g)
    })
    //graph is constructed
    //TODO: now we need to find our synthesized database
  }

}
