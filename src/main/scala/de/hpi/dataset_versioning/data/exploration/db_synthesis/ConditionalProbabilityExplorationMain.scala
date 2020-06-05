package de.hpi.dataset_versioning.data.exploration.db_synthesis

import java.io.PrintWriter
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.data.exploration.db_synthesis.ColnameHistogram.args
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.io.IOService
import org.apache.commons.math3.util
import org.apache.commons.math3.util.ArithmeticUtils

import scala.collection.mutable

object ConditionalProbabilityExplorationMain extends App with StrictLogging {

  IOService.socrataDir = args(0)
  val pr = new PrintWriter(args(1))
  val lineageSizeFilter = args(2).toInt
  val significanceThreshold = 0.95
  val conditionalProbabilityThreshold = 0.9
  var histories = DatasetVersionHistory.fromJsonObjectPerLineFile(IOService.getCleanedVersionHistoryFile().getAbsolutePath)
  histories = histories.filter(_.versionsWithChanges.size > lineageSizeFilter)
  val idToVersionsFlat = histories.map(h => (h.id,h.versionsWithChanges.filter(_ != IOService.STANDARD_TIME_FRAME_START).toSet))
    .toMap
  val md = IOService.getOrLoadCustomMetadataForStandardTimeFrame()

  var byDomain = idToVersionsFlat
    .groupBy{case (k,list) => {
      val firstVersion = DatasetInstance(k, list.head)
      if(!md.metadata.contains(firstVersion))
        null
      else
        md.metadata(firstVersion).topLevelDomain
    }}
  logger.debug(s"found ${byDomain.getOrElse(null,Map()).size} datasets with no url")
  byDomain = byDomain.filter(_._1!=null)
  pr.println("Domain,A,B,P(A),P(B),P(A AND B),P(A|B),P(B|A),significance")
  logger.debug(s"found ${byDomain.size} top level domains")
  byDomain.foreach{case (domain,idToVersions) => {
    //TODO: partition idToVersions
    logger.debug(s"handling domain $domain which contains ${idToVersions.size} datasets")
    val keys = idToVersions.keySet.toIndexedSeq.sorted
    for(i <- 0 until keys.size){
      for(j <- (i+1) until keys.size){
        val curKeyA = keys(i)
        val curKeyB = keys(j)
        //first variant:
        val bOccurrences = idToVersions(curKeyB)
        val aOccurrences = idToVersions(curKeyA)
        val p_B = bOccurrences.size
        val p_A = aOccurrences.size
        val p_A_AND_B = bOccurrences.intersect(aOccurrences).size
        val probAIfB = p_A_AND_B / p_B.toDouble
        val probBIfA = p_A_AND_B / p_A.toDouble
        val u = 181 -7 // these are the points in time where we can have changes (182 days minus first day, minus 7 days where there were errors)
        val significance = 1.0 - p_n_m_k_u(p_A,p_B,p_A_AND_B,u)
        val hasHighCorrelation = probAIfB > conditionalProbabilityThreshold || probBIfA > conditionalProbabilityThreshold
        if(significance>significanceThreshold && hasHighCorrelation)
          pr.println(s"$domain,$curKeyA,$curKeyB,$p_A,$p_B,$p_A_AND_B,$probAIfB,$probBIfA,$significance")
      }
      //logger.trace(s"Finished ${i+1} out of ${keys.size} iterations (${i/keys.size.toFloat}%)")
    }
  }}
  pr.close()


  def p_n_m_k_u(n:Int, m:Int,k:Int,u:Int):Double =  {
    (ArithmeticUtils.binomialCoefficientDouble(m,k)*ArithmeticUtils.binomialCoefficientDouble(u-m,n-k)) / ArithmeticUtils.binomialCoefficientDouble(u,n)
  }
}
