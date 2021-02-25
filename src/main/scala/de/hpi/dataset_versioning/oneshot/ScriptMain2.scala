package de.hpi.dataset_versioning.oneshot

import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.{SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.oneshot.DBSIIExerciseExport.args
import de.hpi.dataset_versioning.oneshot.ScriptMain2.{assocaitionID, bcnfID, id, rowIndex}

import scala.collection.mutable

object ScriptMain2 extends App {
  //TupleReference(2kfw-zvte.1_0(SK8, trip_distance),555)
  //TupleReference(m6dm-c72p.1_0(SK9, trip_id),626)
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  var id = "fg6s-gzvg"
  var bcnfID = 0
  var assocaitionID = Some(11)
  var rowIndex = 83
//  val t = getTable("tfm3-3j95",3,Some(1))
//  val newID = DecomposedTemporalTableIdentifier(subdomain,"tfm3-copy",3,Some(1))
//  val tNEw = new SurrogateBasedSynthesizedTemporalDatabaseTableAssociation(newID.viewID,
//    mutable.HashSet(),
//    mutable.HashSet(newID),
//    t.key,
//    t.nonKeyAttribute,
//    t.foreignKeys,
//    t.rows
//  )
//  tNEw.writeToStandardOptimizationInputFile
//  val tSketch = getTableSketch("tfm3-3j95",3,Some(1))
//  val tSketchNew = new SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch(newID.viewID,
//    mutable.HashSet(),
//    mutable.HashSet(newID),
//    tSketch.key,
//    tSketch.nonKeyAttribute,
//    tSketch.foreignKeys,
//    tSketch.rows
//  )
//  tSketchNew.writeToStandardOptimizationInputFile()

  def getTable(id: String, bcnfID: Int, assocaitionID: Some[Int]) = {
    val table = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
      .loadFromStandardOptimizationInputFile(DecomposedTemporalTableIdentifier(subdomain, id, bcnfID, assocaitionID))
    table
  }

  def getTableSketch(id: String, bcnfID: Int, assocaitionID: Some[Int]) = {
    val table = SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch
      .loadFromStandardOptimizationInputFile(DecomposedTemporalTableIdentifier(subdomain, id, bcnfID, assocaitionID))
    table
  }

  def getTuple(id: String, bcnfID: Int, assocaitionID: Some[Int], rowIndex: Int) = {
    val table = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
      .loadFromStandardOptimizationInputFile(DecomposedTemporalTableIdentifier(subdomain, id, bcnfID, assocaitionID))
    val a = table.getDataTuple(rowIndex)
    a
  }

  val tuple1 = getTuple(id,bcnfID,assocaitionID,rowIndex)
  printTuple
  printTupleSketch
  id = "fg6s-gzvg"
  bcnfID = 0
  assocaitionID = Some(14)
  rowIndex = 864
  printTuple
  printTupleSketch

  val tuple2 = getTuple(id,bcnfID,assocaitionID,rowIndex)
  val a = tuple1.head.getOverlapEvidenceCount(tuple2.head)
  val res = tuple1.head.tryMergeWithConsistent(tuple2.head)
  val cc1 = GLOBAL_CONFIG.CHANGE_COUNT_METHOD.countFieldChangesSimple(tuple1)
  val cc2 = GLOBAL_CONFIG.CHANGE_COUNT_METHOD.countFieldChangesSimple(tuple2)
  val ccRes = GLOBAL_CONFIG.CHANGE_COUNT_METHOD.countFieldChangesSimple(IndexedSeq(res.get))
  println()

  def printTupleSketch = {
    val table = SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch
      .loadFromStandardOptimizationInputFile(DecomposedTemporalTableIdentifier(subdomain, id, bcnfID, assocaitionID))
    val a = table.getDataTuple(rowIndex)
    println(a)
  }

  private def printTuple = {
    val table = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
      .loadFromStandardOptimizationInputFile(DecomposedTemporalTableIdentifier(subdomain, id, bcnfID, assocaitionID))
    val a = table.getDataTuple(rowIndex)
    println(a)
  }

}
