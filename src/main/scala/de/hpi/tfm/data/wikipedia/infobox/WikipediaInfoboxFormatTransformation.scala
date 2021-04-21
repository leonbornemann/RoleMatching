package de.hpi.tfm.data.wikipedia.infobox

import com.typesafe.scalalogging.StrictLogging

import java.io.{File, PrintWriter}

object WikipediaInfoboxFormatTransformation extends App with StrictLogging{
  val inputDir = args(0)
  val outputDir = args(1)
  val statFile = args(2)
  val files = new File(inputDir).listFiles()
  val statisticsGatherer = new WikipediaInfoboxStatistiicsGatherer(new File(statFile))
  logger.debug(s"found ${files.size} files")
  var doneWith =0
  files.foreach(f => {
    val histories = PaddedInfoboxHistory.fromJsonObjectPerLineFile(f.getAbsolutePath)
    val vhs = histories.flatMap(_.asWikipediaInfoboxValueHistories)
    statisticsGatherer.addToFile(vhs)
    val pr = new PrintWriter(outputDir + s"/${f.getName}" )
    vhs.foreach(_.appendToWriter(pr,false,true))
    pr.close()
    doneWith+=1
    if(doneWith%50==0)
      logger.debug(s"done with $doneWith files")
  })
  statisticsGatherer.closeFile()
}
