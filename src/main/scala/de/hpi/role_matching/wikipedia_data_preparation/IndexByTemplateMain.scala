package de.hpi.role_matching.wikipedia_data_preparation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.wikipedia_data_preparation.original_infobox_data.InfoboxRevisionHistory
import de.hpi.role_matching.wikipedia_data_preparation.transformed.WikipediaRoleLineage
import de.hpi.util.GLOBAL_CONFIG

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.io.Source

object IndexByTemplateMain extends App with StrictLogging {
  println(InfoboxRevisionHistory.TIME_AXIS.size)
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val templateNames = Source.fromFile(args(0)).getLines().toSet
  val infoboxHistoryDir = new File(args(1))
  val templateDir = new File(args(2))
  val trainTimeEnd = LocalDate.parse(args(3))
  templateDir.mkdirs()
  val files = infoboxHistoryDir.listFiles()
  logger.debug(s"Found ${files.size} files")
  var processed = 0
  val templateFileWriters = templateNames
    .map(tn => (tn, new PrintWriter(s"${templateDir.getAbsolutePath}/$tn.json")))
    .toMap

  val fulfillsFilter = files.toIndexedSeq.foreach(f => {
    logger.debug(s"processing ${f.getAbsolutePath}")
    val res = WikipediaRoleLineage.fromJsonObjectPerLineFile(f.getAbsolutePath)
      .withFilter(wiwh => wiwh.template.isDefined && templateNames.contains(wiwh.template.get) && wiwh.isOfInterest(trainTimeEnd)) //all query strings need to be matched in at least one value
      .foreach(wiwh => wiwh.appendToWriter(templateFileWriters(wiwh.template.get), false, true))
    processed += 1
    if (processed % 100 == 0)
      logger.debug(s"finished $processed")
    res
  })
  templateFileWriters.values.foreach(_.close())
}
