package de.hpi.role_matching.evaluation.runtime

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import scala.io.Source

object RuntimeFromLogsMain extends App {
  val dsNames = IndexedSeq(
    "austintexas",
    "chicago",
    "education",
    "football",
    "gov.maryland",
    "military",
    "oregon",
    "politics",
    "tv_and_film",
    "utah"
  )

  def getLocalDateTime(start: String) = {
    val formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss.SSS")
    LocalDateTime.parse(start,formatter)
  }

  private val timesForSGCP = IndexedSeq(
    ("27.09.2021_21:39:33.714", "27.09.2021_22:05:12.139"),
    ("27.09.2021_12:32:50.267", "27.09.2021_12:37:02.231"),
    ("28.09.2021_16:43:05.975", "28.09.2021_18:03:54.191"),
    ("27.09.2021_22:58:49.065", "28.09.2021_10:01:25.024"),
    ("27.09.2021_22:05:53.761", "27.09.2021_22:07:53.892"),
    ("27.09.2021_22:25:44.245", "27.09.2021_22:38:05.092"),
    ("27.09.2021_22:06:13.148", "27.09.2021_22:06:29.022"),
    ("27.09.2021_22:20:01.513", "27.09.2021_22:23:42.579"),
    ("28.09.2021_10:22:06.438", "28.09.2021_16:31:15.233"),
    ("27.09.2021_22:07:11.730", "27.09.2021_22:18:52.087")
  )
  val times = dsNames.zip(
    timesForSGCP.map{case (start,end) => ChronoUnit.SECONDS.between(getLocalDateTime(start),getLocalDateTime(end)) / 3600.0}
  ).toMap

  def readRunTimes(getAbsolutePath: String) = {
    val f = if (new File(getAbsolutePath + "/alpha_3.1E-5.log").exists()) new File(getAbsolutePath + "/alpha_3.1E-5.log") else new File(getAbsolutePath + "/alpha_5.18E-4.log")
    val timeInSeconds = Source.fromFile(f)
      .getLines()
      .toIndexedSeq
      .tail
      .map(l => {
        l.split("\t")(3).toDouble
      })
      .sum
    (timeInSeconds / 3600.0) / 12.0 //12 threads
  }

  val runtimesMDMCP = new File("/home/leon/data/dataset_versioning/optimizationRuntime/")
    .listFiles()
    .filter(_.isDirectory)
    .map(dir => {
      (dir.getName,readRunTimes(dir.getAbsolutePath))
    })
    .toMap

  times.map{case (key,time) => (key,"& %.3f ".format(time+runtimesMDMCP(key)))}
    .foreach(println)
//  val optimizationCatFile = s"$runtimeDir/allLogsSparseGraphCliquePartitioning.txt"
//  val lines = Source.fromFile(optimizationCatFile).getLines().toIndexedSeq
//    .map(s => if(s.contains("Called with ArraySeq")) (s,true) else (s,false))
//    .zipWithIndex
//  val startAt = "Starting Clique Partitioning Optimization"
//  val endBefore = "Could NOT find resource [logback-test.xml]"
//  lines
//    .withFilter(_._1._2)
//    .foreach{case ((l,isStart),i) => {
//      assert(isStart)
//      val tokens = l.split("/")
//      val dsName = tokens(tokens.indexOf("hybridOptimizerResult")+1)
//      println(dsName)
//    }}
}
