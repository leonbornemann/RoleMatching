package de.hpi.dataset_versioning.db_synthesis.baseline

import java.io.PrintWriter

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTable
import de.hpi.dataset_versioning.io.IOService

object DetailedBCNFChangeCounting extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val inspectionID = "97t6-zrhs"

  def doIDExploration = {
    val tt = TemporalTable.load(inspectionID)
    val allBCNFTables = DecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain, inspectionID)
      .map(dtt => tt.project(dtt).projection)
    println()
    val colsInOriginal = tt.getTemporalColumns()
    val attrSet = tt.attributes.map(_.attrId)
    val insertTime = tt.insertTime
    println("attrID,#ChangesTT,#ChangesBCNF")
    val fieldsWithChanges = colsInOriginal
      .map(tc => (tc.attrID,tc.fieldLineages))
      .flatMap{case (aID,f) => f.zipWithIndex
        .map{case (fl,i) => (i,aID,fl.countChanges(insertTime,GLOBAL_CONFIG.CHANGE_COUNT_METHOD))}}
      //.filter(_._3>1)
    fieldsWithChanges.sorted.foreach(println)
    println()
    val bcnfCols = allBCNFTables.flatMap(bcnf => bcnf.getTemporalColumns().map(tc => (tc.attrID,bcnf.dtt.get.id.bcnfID,tc)))
    attrSet.foreach(attrID => {
      val tc = colsInOriginal.filter(_.attrID==attrID).head
      val changesTT = tc.fieldLineages.map(_.countChanges(insertTime,GLOBAL_CONFIG.CHANGE_COUNT_METHOD)).sum
      val changesBCNF = bcnfCols
        .filter(_._1==attrID)
        .flatMap(_._3.fieldLineages)
        .map(_.countChanges(insertTime,GLOBAL_CONFIG.CHANGE_COUNT_METHOD))
        .sum
      println(attrID,changesTT,changesBCNF)
    })
    allBCNFTables.foreach(tt => {
      println(tt.id)
      println(tt.dtt.get.originalFDLHS.map(a => (a.attrId,a.lastName,a.activeTimeIntervals)))
      println(tt.getTemporalColumns().map(tc => (tc.attributeLineage.lastName,tc.cardinality)))
    })
  }

  //doIDExploration

  doBCNFCounting

  private def doBCNFCounting = {
//    val ids = DatasetInfo.getViewIdsInSubdomain(subdomain)
//    val idsWithDecomposedTables = DecomposedTemporalTable.filterNotFullyDecomposedTables(subdomain, ids)
//    val res = new PrintWriter("changes.csv")
//    //val toFilter = "kf7e-cur8\nn4j6-wkkf\nygr5-vcbg\nsxs8-h27x\ncygx-ui4j\npubx-yq2d\ni9rk-duva\n22u3-xenr\nx2n5-8w5q".split("\n")
//    res.println("id,nAttr,nTuplesWithChanges,nchangesView,nchangesBCNF,nrowView,nrowBCNF,nFieldView,nFieldBCNF")
//    val bcnFTableWriter = new PrintWriter("")
//    idsWithDecomposedTables
//      //.filter(!toFilter.contains(_))
//      .foreach(id => {
//      val tt = TemporalTable.load(id)
//      val allBCNFTables = DecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain, id)
//        .map(dtt => tt.project(dtt).projection)
//      val colsInOriginal = tt.getTemporalColumns()
//      val insertTime = tt.insertTime
//      val fieldsWithChanges = colsInOriginal
//        .map(tc => (tc.attrID,tc.fieldLineages))
//        .flatMap{case (aID,f) => f.zipWithIndex
//          .map{case (fl,i) => (i,aID,fl.countChanges(insertTime,GLOBAL_CONFIG.CHANGE_COUNT_METHOD))}}
//      val nTuplesWithMoreThanOneChange = fieldsWithChanges.filter(_._3>1).map(_._1).toSet.size
//      val changesTT = tt.countChanges(GLOBAL_CONFIG.CHANGE_COUNT_METHOD)
//      val changesBCNF = allBCNFTables.map(bcnf => bcnf.countChanges(GLOBAL_CONFIG.CHANGE_COUNT_METHOD)).sum
//      val nrowView = tt.rows.size
//      val nrowBCNF = allBCNFTables.map(_.rows.size).sum
//      val nFieldView = tt.attributes.size*tt.rows.size
//      val nFieldBCNF = allBCNFTables.map(dtt => dtt.attributes.size*dtt.rows.size).sum
//      val string = s"$id,${tt.attributes.size},$nTuplesWithMoreThanOneChange,$changesTT,$changesBCNF,$nrowView,$nrowBCNF,$nFieldView,$nFieldBCNF"
//      println(string)
//      res.println(string)
//    })
//    res.close()
  }
}
