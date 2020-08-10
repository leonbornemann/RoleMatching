package de.hpi.dataset_versioning.oneshot

import java.io.{File, FileReader}
import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.{AttributeLineage, ChangeCube, ChangeExporter, TemporalTable}
import de.hpi.dataset_versioning.data.matching.ColumnMatchingRefinement
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.TemporalSchema
import de.hpi.dataset_versioning.data.simplified.RelationalDataset
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.fd.FunctionalDependencySet
import de.hpi.dataset_versioning.db_synthesis.top_down_no_change.decomposition.DatasetInfo
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}
import org.apache.commons.csv.{CSVFormat, CSVParser}

import scala.collection.mutable
import scala.io.Source

object ScriptMain extends App {

  //
  //  TODO: get all events where the intersection of two namesets of column lineages is non-empty, and they have at least one point in time where they exist in common
//    TODO: if not many then add column matching stage
  IOService.socrataDir = args(0)
  val subdomain = args(1)
    val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
    val subdomainIds = subDomainInfo(subdomain)
      .map(_.id)
      .toIndexedSeq

  val allTimestamps = (IOService.STANDARD_TIME_FRAME_START.toEpochDay to IOService.STANDARD_TIME_FRAME_END.toEpochDay)
    .toIndexedSeq
    .map(LocalDate.ofEpochDay(_))

  def sharesAtLeast1Timestamp(attrA: AttributeLineage, attrB: AttributeLineage) = {
      allTimestamps.exists(ts => attrA.valueAt(ts)._2.exists && attrB.valueAt(ts)._2.exists)
  }

  var numPossibleNewAttrMatches = 0
  var numPossibleConfusions = 0
  var dsWithPotentialMatch = mutable.HashMap[String,mutable.HashSet[Set[AttributeLineage]]]()
  subdomainIds.foreach(id => {
    val schema = TemporalSchema.load(id)
    val attrs = schema.attributes
    for(i <- 0 until attrs.size){
      val attrA = attrs(i)
      for(j <- (i+1) until attrs.size){
        val attrB = attrs(j)
        if(attrB.nameSet.intersect(attrA.nameSet).size!=0){
          dsWithPotentialMatch.getOrElseUpdate(id,mutable.HashSet())
            .add(Set(attrA,attrB))
          if(sharesAtLeast1Timestamp(attrA,attrB)){
            numPossibleConfusions +=1
          } else{
            numPossibleNewAttrMatches +=1
          }
        }
      }
    }
  })
  println(numPossibleConfusions)
  println(numPossibleNewAttrMatches)

  var totalMergesFound = 0
  subdomainIds.foreach(id => {
    val schema = TemporalSchema.load(id)
    val attrs = schema.attributes
    val extender = new ColumnMatchingRefinement(id)
    val (mergesFound,result) = extender.getPossibleSaveMerges(attrs)
    if(dsWithPotentialMatch.contains(id) && mergesFound == 0 ){
      val tt = TemporalTable.load(id)
      println()
    }
    val (mergesFound2,result2) = extender.getPossibleSaveMerges(attrs)
    totalMergesFound += mergesFound
  })
  println(totalMergesFound)
  //LHS-size (key size) chosen by decomposition
//  IOService.socrataDir = args(0)
//  val subdomain = args(1)
//  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
//  val subdomainIds = subDomainInfo(subdomain)
//    .map(_.id)
//    .toIndexedSeq
//  val temporallyDecomposedTables = subdomainIds
//    .filter(id => DBSynthesis_IOService.getDecomposedTemporalTableDir(subdomain,id).exists())
//    .flatMap(id => {
//      val dtts = DecomposedTemporalTable.loadAll(subdomain, id)
//      dtts
//    })
//  temporallyDecomposedTables.map(_.originalFDLHS.size).groupBy(identity)
//    .toIndexedSeq
//    .sortBy(_._1)
//    .foreach(t => println(s"${t._1},${t._2.size}"))

  //FD discovery stats:
//  val lines = Source.fromFile("/home/leon/Desktop/statsNew.csv")
//    .getLines()
//    .toSeq
//    .tail
//    .map(_.split(",").map(_.toInt).toIndexedSeq)
//    .filter(_(0)!=0)
//  println(lines)
//  val fdIntersectionSelectivity = lines.map(l => l(1) / l(0).toDouble)
//  var relativeOverlap = lines.filter(l => l(0) != l(1)).map(l => l(4) / l(2).toDouble)
//  relativeOverlap = relativeOverlap ++ (0 until 42).map(_ => 1.0)
//  val avgOverlap = relativeOverlap.sum / relativeOverlap.size.toDouble
//  println(avgOverlap)
//  var fdIntersectionSelectivityWithoutEquality = lines.filter(l => l(0) != l(1)).map(l => l(1) / l(0).toDouble)
//  //account for thos with changes that are equal
//  fdIntersectionSelectivityWithoutEquality= fdIntersectionSelectivityWithoutEquality ++ (0 until 42).map(_ => 1.0)
//
//  println(fdIntersectionSelectivity.sum / fdIntersectionSelectivity.size.toDouble)
//  println(fdIntersectionSelectivity.size)
//  println(fdIntersectionSelectivityWithoutEquality.sum / fdIntersectionSelectivityWithoutEquality.size.toDouble)
//  println(fdIntersectionSelectivityWithoutEquality.size)
//
//  println(
//  )
//  fdIntersectionSelectivityWithoutEquality.foreach(println(_))
}
