package de.hpi.dataset_versioning.db_synthesis.baseline

import java.io.PrintWriter

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.db_synthesis.baseline.config.{AllDeterminantIgnoreChangeCounter, DatasetAndRowInitialInsertIgnoreFieldChangeCounter, DatasetInsertIgnoreFieldChangeCounter, FieldChangeCounter, GLOBAL_CONFIG, NormalFieldChangeCounter}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.{DecomposedTemporalTable, SurrogateBasedDecomposedTemporalTable}
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

object DetailedBCNFChangeCounting extends App with StrictLogging{

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val inspectionID = "97t6-zrhs"

  //doIDExploration

  //doBCNFCounting

//  private def doBCNFCounting = {
//    val ids = DatasetInfo.getViewIdsInSubdomain(subdomain)
//    val idsWithDecomposedTables = SurrogateBasedDecomposedTemporalTable.filterNotFullyDecomposedTables(subdomain, ids)
//    val specialCounter = new PKIgnoreChangeCounter(subdomain,new DatasetAndRowInitialInsertIgnoreFieldChangeCounter)
//    val changeCounters = Seq(new DatasetAndRowInitialInsertIgnoreFieldChangeCounter(),
//      new DatasetInsertIgnoreFieldChangeCounter(),
//      new NormalFieldChangeCounter(),
//      //PK Ignore:
//      new PKIgnoreChangeCounter(subdomain,new DatasetAndRowInitialInsertIgnoreFieldChangeCounter),
//      new PKIgnoreChangeCounter(subdomain,new DatasetInsertIgnoreFieldChangeCounter),
//      new PKIgnoreChangeCounter(subdomain,new NormalFieldChangeCounter),
//      //GLobal DeterminantIgnore:
//      new AllDeterminantIgnoreChangeCounter(new DatasetAndRowInitialInsertIgnoreFieldChangeCounter),
//      new AllDeterminantIgnoreChangeCounter(new DatasetInsertIgnoreFieldChangeCounter),
//      new AllDeterminantIgnoreChangeCounter(new NormalFieldChangeCounter))
//    val ccToWriter = changeCounters.map(cc => (cc,new PrintWriter(s"${cc.name}.txt"))).toMap
//    //val toFilter = "kf7e-cur8\nn4j6-wkkf\nygr5-vcbg\nsxs8-h27x\ncygx-ui4j\npubx-yq2d\ni9rk-duva\n22u3-xenr\nx2n5-8w5q".split("\n")
//    //res.println("id,nAttr,nTuplesWithChanges,nchangesView,nchangesBCNF,nrowView,nrowBCNF,nFieldView,nFieldBCNF")
//    //val bcnFTableWriter = new PrintWriter("")
//    idsWithDecomposedTables
//      //.filter(!toFilter.contains(_))
//      .foreach(id => {
//        val tt = TemporalTable.load(id)
//        val allBCNFTables = SurrogateBasedDecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain, id)
//          .map(dtt => tt.project(dtt).projection)
//        val columnsINBCNFCounted = allBCNFTables.map(_.dtt.get.attributes.size).sum
//        val columnsInTTCounted = tt.attributes.size - specialCounter.getOriginalPK(allBCNFTables.map(_.dtt.get)).size
//        if(columnsInTTCounted != columnsINBCNFCounted) {
//          println(s"$id")
//        }
//        //assert(columnsInTTCounted==columnsINBCNFCounted)
//        ccToWriter.foreach{case (cc,writer) =>{
//          ??? //TODO: the following no longer works but will work again - just remove the key param
////          val changesOriginal = tt.countChanges(cc,key)
////          val changesInBCNF = allBCNFTables.map(bcnf => bcnf.countChanges(cc,key)).sum
////          writer.println(s"$id:${changesInBCNF-changesOriginal}")
//        }}
//    })
//    ccToWriter.foreach(_._2.close())
//  }

  case class ColumnChangeReport(cc: FieldChangeCounter, countPk: Boolean,countNonPK:Boolean, writer: PrintWriter)

}
