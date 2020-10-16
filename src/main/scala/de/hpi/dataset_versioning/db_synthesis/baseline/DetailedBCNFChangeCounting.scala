package de.hpi.dataset_versioning.db_synthesis.baseline

import java.io.PrintWriter

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.db_synthesis.baseline.config.{AllDeterminantIgnoreChangeCounter, DatasetAndRowInitialInsertIgnoreFieldChangeCounter, DatasetInsertIgnoreFieldChangeCounter, FieldChangeCounter, GLOBAL_CONFIG, NormalFieldChangeCounter}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTable
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

object DetailedBCNFChangeCounting extends App with StrictLogging{

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

  //doBCNFCounting
  doPkChangeCounting


  def doPkChangeCounting = {
    val ids = DatasetInfo.getViewIdsInSubdomain(subdomain)
    val idsWithDecomposedTables = DecomposedTemporalTable.filterNotFullyDecomposedTables(subdomain, ids)
    val fieldChangeCounters = Seq(new DatasetAndRowInitialInsertIgnoreFieldChangeCounter(),
      new DatasetInsertIgnoreFieldChangeCounter(),
      new NormalFieldChangeCounter())
    val countDeterminantCounters = fieldChangeCounters.map(cc => ColumnChangeReport(cc,true,false,new PrintWriter(cc.name + "_DeterminantOnly")))
    val countNonDeterminantCounters = fieldChangeCounters.map(cc => ColumnChangeReport(cc,false,true,new PrintWriter(cc.name + "_NonDeterminantOnly")))
    idsWithDecomposedTables.foreach(id =>{
      val tt = TemporalTable.load(id)
      val insertTime = tt.insertTime
      val allBCNFTables = DecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain, id)
        .map(dtt => tt.project(dtt).projection)
      val determinants = allBCNFTables.flatMap(_.dtt.get.primaryKey.map(_.attrId).toSet).toSet
      tt.getTemporalColumns().foreach(tc => {
        val isDeterminant = determinants.contains(tc.attrID)
        if(isDeterminant){
          countDeterminantCounters.foreach(cct => {
            cct.writer.println(cct.cc.countColumnChanges(tc,insertTime,isDeterminant) / tt.rows.size.toDouble)
          })
        } else {
          countNonDeterminantCounters.foreach(cct => {
            cct.writer.println(cct.cc.countColumnChanges(tc,insertTime,isDeterminant) / tt.rows.size.toDouble)
          })
        }
      })
    })
    (countDeterminantCounters ++ countNonDeterminantCounters).foreach(_.writer.close())
  }

  private def doBCNFCounting = {
    val ids = DatasetInfo.getViewIdsInSubdomain(subdomain)
    val idsWithDecomposedTables = DecomposedTemporalTable.filterNotFullyDecomposedTables(subdomain, ids)
    val specialCounter = new PKIgnoreChangeCounter(subdomain,new DatasetAndRowInitialInsertIgnoreFieldChangeCounter)
    val changeCounters = Seq(new DatasetAndRowInitialInsertIgnoreFieldChangeCounter(),
      new DatasetInsertIgnoreFieldChangeCounter(),
      new NormalFieldChangeCounter(),
      //PK Ignore:
      new PKIgnoreChangeCounter(subdomain,new DatasetAndRowInitialInsertIgnoreFieldChangeCounter),
      new PKIgnoreChangeCounter(subdomain,new DatasetInsertIgnoreFieldChangeCounter),
      new PKIgnoreChangeCounter(subdomain,new NormalFieldChangeCounter),
      //GLobal DeterminantIgnore:
      new AllDeterminantIgnoreChangeCounter(new DatasetAndRowInitialInsertIgnoreFieldChangeCounter),
      new AllDeterminantIgnoreChangeCounter(new DatasetInsertIgnoreFieldChangeCounter),
      new AllDeterminantIgnoreChangeCounter(new NormalFieldChangeCounter))
    val ccToWriter = changeCounters.map(cc => (cc,new PrintWriter(s"${cc.name}.txt"))).toMap
    //val toFilter = "kf7e-cur8\nn4j6-wkkf\nygr5-vcbg\nsxs8-h27x\ncygx-ui4j\npubx-yq2d\ni9rk-duva\n22u3-xenr\nx2n5-8w5q".split("\n")
    //res.println("id,nAttr,nTuplesWithChanges,nchangesView,nchangesBCNF,nrowView,nrowBCNF,nFieldView,nFieldBCNF")
    //val bcnFTableWriter = new PrintWriter("")
    idsWithDecomposedTables
      //.filter(!toFilter.contains(_))
      .foreach(id => {
        val tt = TemporalTable.load(id)
        val allBCNFTables = DecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain, id)
          .map(dtt => tt.project(dtt).projection)
        val columnsINBCNFCounted = allBCNFTables.map(_.dtt.get.nonKeyAttributeLineages.size).sum
        val columnsInTTCounted = tt.attributes.size - specialCounter.getOriginalPK(allBCNFTables.map(_.dtt.get)).size
        if(columnsInTTCounted != columnsINBCNFCounted) {
          println(s"$id")
        }
        //assert(columnsInTTCounted==columnsINBCNFCounted)
        val key = allBCNFTables.flatMap(_.dtt.get.primaryKey).map(_.attrId).toSet
        ccToWriter.foreach{case (cc,writer) =>{
          val changesOriginal = tt.countChanges(cc,key)
          val changesInBCNF = allBCNFTables.map(bcnf => bcnf.countChanges(cc,key)).sum
          writer.println(s"$id:${changesInBCNF-changesOriginal}")
        }}
    })
    ccToWriter.foreach(_._2.close())
  }

  case class ColumnChangeReport(cc: FieldChangeCounter, countPk: Boolean,countNonPK:Boolean, writer: PrintWriter)

}
