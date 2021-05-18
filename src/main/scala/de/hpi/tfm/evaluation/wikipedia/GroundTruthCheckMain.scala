package de.hpi.tfm.evaluation.wikipedia

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.wikipedia.infobox.transformed.WikipediaInfoboxValueHistory

import java.io.File
import scala.io.Source

object GroundTruthCheckMain extends App with StrictLogging{
  val file = args(0)
  val pageDir = new File(args(1))
  val properties = Source.fromFile(file)
    .getLines()
    .toIndexedSeq
    .tail
    .map(l => {
      val tokens = l.split(",")
      val rowID = tokens(0).toInt
      val id1 = BigInt(tokens(2))
      val p1 = tokens(3)
      val id2 = BigInt(tokens(5))
      val p2 = tokens(6)
      (rowID,id1,p1,id2,p2)
    })
  val res = properties.map{case (rID,id1,p1,id2,p2) => {
    val file1Option = WikipediaInfoboxValueHistory.findFileForID(pageDir,id1)
    val file2Option = WikipediaInfoboxValueHistory.findFileForID(pageDir,id2)
    if(file1Option.isDefined && file2Option.isDefined){
      val file1=  file1Option.get
      val file2=  file2Option.get
      val valueHistories1 = WikipediaInfoboxValueHistory.fromJsonObjectPerLineFile(file1.getAbsolutePath)
        .filter(vh => vh.pageID==id1 && vh.p==p1)
      val valueHistories2 = WikipediaInfoboxValueHistory.fromJsonObjectPerLineFile(file2.getAbsolutePath)
        .filter(vh => vh.pageID==id2 && vh.p==p2)
      if(valueHistories1.size!=1){
        logger.debug(s"Did not find ${(id1,p1)} in file $file1")
        (rID,false)
      } else if(valueHistories2.size!=1){
        logger.debug(s"Did not find ${(id2,p2)} in file $file2")
        (rID,false)
      } else {
        val vh1 = valueHistories1.head
        val vh2 = valueHistories2.head
        if(vh1.toIdentifiedFactLineage.factLineage.toFactLineage.tryMergeWithConsistent(vh2.toIdentifiedFactLineage.factLineage.toFactLineage).isDefined){
          logger.debug(s"True for $rID (${(rID,id1,p1,id2,p2)})")
          (rID,true)
        } else {
          logger.debug(s"False for $rID (${(rID,id1,p1,id2,p2)})")
          (rID,false)
        }
      }
    } else {
      logger.debug(s"Did not find file for ${(rID,id1,p1,id2,p2)}")
      (rID,false)
    }
  }}
  println("rID,holds")
  res.foreach(t => println(s"${t._1},${t._2}"))


}
