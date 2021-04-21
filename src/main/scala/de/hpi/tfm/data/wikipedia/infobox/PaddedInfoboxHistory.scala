package de.hpi.tfm.data.wikipedia.infobox

import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{FactLineage, FactLineageWithHashMap}

import java.io.{File, PrintWriter}
import scala.collection.mutable

case class PaddedInfoboxHistory(template: Option[String],
                                pageID: BigInt,
                                pageTitle: String,
                                key: String,
                                lineages: mutable.HashMap[String, FactLineageWithHashMap]) extends JsonWritable[PaddedInfoboxHistory]{
  def asWikipediaInfoboxValueHistories = lineages
    .map{case (p,h) => WikipediaInfoboxValueHistory(template,pageID,key,p,h)}
    .toIndexedSeq


  def appendToBucketFile(bucketFileWriter:PrintWriter) = {
    appendToWriter(bucketFileWriter,false,true)
  }

}

object PaddedInfoboxHistory extends JsonReadable[PaddedInfoboxHistory]{

}

