package de.hpi.tfm.data.wikipedia.infobox.statistics

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.wikipedia.infobox.transformed.WikipediaInfoboxValueHistory

import java.io.{File, PrintWriter}
import scala.util.Random

object RerunStatisticsExtractionMain extends App with StrictLogging{
  val infoboxHistoryDir = new File(args(0))
  val statisticsResultWriter =  new PrintWriter(args(1))
  val statisticsResultWriterSample = new PrintWriter(args(2))
  val sampleProbability = args(3).toDouble
  val files = infoboxHistoryDir.listFiles()
  logger.debug(s"Found ${files.size} files")
  var processed = 0
  val random = new Random()
  statisticsResultWriter.println(WikipediaInfoboxStatisticsLine.getSchema)
  statisticsResultWriterSample.println(WikipediaInfoboxStatisticsLine.getSchema)
  files.foreach(f => {
    logger.debug(s"processing ${f.getAbsolutePath}")
    val res = WikipediaInfoboxValueHistory.fromJsonObjectPerLineFile(f.getAbsolutePath)
    val lines = res.map(_.toWikipediaInfoboxStatisticsLine)
    lines.foreach(l => statisticsResultWriter.println(l.getCSVLine))
    lines.filter(_ => random.nextDouble() <= sampleProbability)
      .foreach(l => statisticsResultWriterSample.println(l.getCSVLine))
    processed += 1
    if (processed % 100 == 0)
      logger.debug(s"finished $processed")
    res
  })
  statisticsResultWriterSample.close()
  statisticsResultWriter.close()
}
