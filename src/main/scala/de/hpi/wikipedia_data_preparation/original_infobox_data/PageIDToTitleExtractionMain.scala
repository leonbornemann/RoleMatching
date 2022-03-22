package de.hpi.wikipedia_data_preparation.original_infobox_data

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.cbrm.data.Util
import de.hpi.wikipedia_data_preparation.transformed.WikipediaRoleLineage

import java.io.{File, PrintWriter}

object PageIDToTitleExtractionMain extends App with StrictLogging{
  val f = new File(args(0))
  val resultFile = args(0)
  new File(resultFile).mkdirs()
  val mappingFile = new PrintWriter(resultFile + s"/${f.getName}.csv")
  logger.debug(s"Processing $f")
  val objects = InfoboxRevision.fromJsonObjectPerLineFile(f.getAbsolutePath)
  objects.foreach(_.checkIntegrity())
  val revisionHistories = InfoboxRevisionHistory.getFromRevisionCollection(objects)
  revisionHistories.map(rh => (rh.revisions.last.pageID,rh.revisions.last.pageTitle))
    .foreach{case (id,title) => mappingFile.println(s"$id,${Util.toCSVSafe(title)}") }
  mappingFile.close()
}
