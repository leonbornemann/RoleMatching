package de.hpi.role_matching.wikipedia_data_preparation.original_infobox_data

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.data.Util

import java.io.{File, PrintWriter}

object PageIDToTitleExtractionMain extends App with StrictLogging{
  val f = new File(args(0))
  val resultDir = args(1)
  new File(resultDir).mkdirs()
  val mappingFile = new PrintWriter(resultDir + s"/${f.getName}.csv")
  logger.debug(s"Processing $f")
  val objects = InfoboxRevision.fromJsonObjectPerLineFile(f.getAbsolutePath)
  objects.foreach(_.checkIntegrity())
  val revisionHistories = InfoboxRevisionHistory.getFromRevisionCollection(objects)
  revisionHistories.map(rh => (rh.revisions.last.pageID,rh.revisions.last.pageTitle))
    .foreach{case (id,title) => mappingFile.println(s"$id,${Util.toCSVSafe(title)}") }
  mappingFile.close()
}
