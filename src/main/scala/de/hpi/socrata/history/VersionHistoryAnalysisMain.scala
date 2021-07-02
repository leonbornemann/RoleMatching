package de.hpi.socrata.history

import de.hpi.socrata.io.Socrata_IOService

import java.io.PrintWriter

object VersionHistoryAnalysisMain extends App {
  Socrata_IOService.socrataDir = args(0)
  val versionCountHistogramFile = args(1)
  val changeTimeSeriesFile = args(2)
  val list = DatasetVersionHistory.fromJsonObjectPerLineFile(Socrata_IOService.getCleanedVersionHistoryFile().getAbsolutePath)

  statisticalAnalysis

 private def statisticalAnalysis = {
    val a = list.map(_.versionsWithChanges.size)
    val histogram = a.groupBy(identity)
      .mapValues(_.size)
   val prVersionCountHistogram = new PrintWriter(versionCountHistogramFile)
    prVersionCountHistogram.println("Num_versions,num_histories")
    histogram.toIndexedSeq.sortBy(_._1)
      .foreach(t => prVersionCountHistogram.println(t._1 + "," + t._2))
   prVersionCountHistogram.close()
   val prTimeSeries = new PrintWriter(changeTimeSeriesFile)
    val b = list //.filter(_.versionsWithChanges.size>9)
      .flatMap(_.versionsWithChanges)
      .groupBy(identity)
      .mapValues(_.size)
    prTimeSeries.println("Date,num_new_versions")
    b.toIndexedSeq
      .sortBy(_._1.toEpochDay)
      .foreach(t => prTimeSeries.println(t._1 + "," + t._2))
   prTimeSeries.close()
  }
}
