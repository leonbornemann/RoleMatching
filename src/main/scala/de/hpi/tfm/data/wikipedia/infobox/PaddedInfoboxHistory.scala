package de.hpi.tfm.data.wikipedia.infobox

import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{FactLineage, FactLineageWithHashMap}

import java.io.File
import scala.collection.mutable

case class PaddedInfoboxHistory(template: Option[String],
                                pageID: BigInt,
                                pageTitle: String,
                                key: String,
                                lineages: mutable.HashMap[String, FactLineageWithHashMap]) extends JsonWritable[PaddedInfoboxHistory]{

  def writeToDir(dir:File) = {
    val fname = Seq(template.getOrElse(""),pageID,pageTitle,key,".json").mkString("_")
    toJsonFile(new File(dir.getAbsolutePath + s"/$fname"))
  }

}

object PaddedInfoboxHistory extends JsonReadable[PaddedInfoboxHistory]{

}

