package de.hpi.wikipedia_data_preparation.original_infobox_data

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.cbrm.data.Util
import de.hpi.wikipedia_data_preparation.transformed.WikipediaRoleLineage

import java.io.{File, PrintWriter}

object PageIDToTitleExtractionMain extends App with StrictLogging{
  val infoboxHistoryFiles = new File(args(0)).listFiles()
  val mappingFile = new PrintWriter(args(1))
  val lineagesComplete = infoboxHistoryFiles.foreach(f => {
    logger.debug(s"Processing $f")
    val objects = InfoboxRevision.fromJsonObjectPerLineFile(f.getAbsolutePath)
    objects.foreach(_.checkIntegrity())
    val revisionHistories = InfoboxRevisionHistory.getFromRevisionCollection(objects)
    revisionHistories.map(rh => (rh.revisions.last.pageID,rh.revisions.last.pageTitle))
      .foreach{case (id,title) => mappingFile.println(s"$id,${Util.toCSVSafe(title)}") }
  })
  mappingFile.close()
}
