package de.hpi.wikipedia_data_preparation.transformed

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.cbrm.data.json_serialization.{JsonReadable, JsonWritable}
import de.hpi.role_matching.cbrm.data.{RoleLineage, RoleLineageWithHashMap, RoleLineageWithID}

import java.io.File
import java.time.LocalDate

case class WikipediaRoleLineage(template:Option[String],
                                pageID: BigInt,
                                key: String,
                                p: String,
                                lineage: RoleLineageWithHashMap) extends JsonWritable[WikipediaRoleLineage]{
  def isOfInterest(trainTimeEnd:LocalDate) = {
    lineage.toRoleLineage.isOfInterest(trainTimeEnd)
  }

  def changeCount = {
    val withIndex = lineage.toRoleLineage.lineage
      .filter(t => !RoleLineage.isWildcard(t._2))
      .toIndexedSeq
      .zipWithIndex
    withIndex
      //.withFilter{case ((t,v),i) => !FactLineage.isWildcard(v)}
      .filter{case ((t,v),i) => i==0 || v != withIndex(i-1)}
      .size-1
  }

  def toWikipediaURLInfo = s"https://en.wikipedia.org/?curid=$pageID ($p)"

  def projectToTimeRange(start: LocalDate, end: LocalDate) = {
    WikipediaRoleLineage(template,pageID,key,p,lineage.toRoleLineage.projectToTimeRange(start,end).toSerializationHelper)
  }

  def toIdentifiedFactLineage = {
    val wikipediaID = ExtendedWikipediaRoleID(template,pageID,key,p)
    RoleLineageWithID(wikipediaID.toCompositeID,lineage)
  }

}

object WikipediaRoleLineage extends JsonReadable[WikipediaRoleLineage] with StrictLogging{

  def findFileForID(dir:File,id:BigInt) = {
    val matchingFile = dir
      .listFiles()
      .toIndexedSeq
      .map(f => {
        val tokens = f.getName.split("-")
        (f,BigInt(tokens(0)),BigInt(tokens(1).split("\\.")(0)))
      })
      .filter(t => id <= t._3 && id >= t._2)
    if (matchingFile.size!=1){
      logger.debug(s"Weird for $id, found $matchingFile")
    }
    if(matchingFile.size!=0)
      Some(matchingFile.head._1)
    else {
      None
    }
  }

  def getFilenameForBucket(originalBucketFilename:String) = {
    val pageMin = BigInt(originalBucketFilename.split("xml-p")(1).split("p")(0))
    val pageMax = BigInt(originalBucketFilename.split("xml-p")(1).split("p")(1).split("\\.")(0))
    s"$pageMin-$pageMax.json"
  }
}
