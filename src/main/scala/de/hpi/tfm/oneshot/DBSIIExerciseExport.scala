package de.hpi.tfm.oneshot

import de.hpi.tfm.data.socrata.change.ChangeCube
import de.hpi.tfm.data.socrata.metadata.custom.DatasetInfo
import de.hpi.tfm.io.IOService

import java.io.{File, PrintWriter}
import java.sql.Timestamp
import scala.language.postfixOps

object DBSIIExerciseExport extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
  val subdomainIds = subDomainInfo(subdomain)
    .map(_.id)
    .toIndexedSeq
  val resultDir = new File("/home/leon/Documents/lehre/ws2021/Übungen_DBS_II_WS2019/Aufgabenblatt_07/Data_Spark/")
  var curResultPR = new PrintWriter(resultDir + "/part_" + curFilenameCounter + ".csv")
  curResultPR.println("Dataset_ID,Timestamp,EntityID,AttributeName,newValue")
  var curFilenameCounter = 0
  var curLineCount = 0

  def sanitizeValue(str: String) = "\"" + str.replace('"','\'')
    .replace(',',';')
    .replace('\n',' ')
    .replace('\r',' ')
    .replace("\r\n"," ") + "\""

  subdomainIds
    .filter(id => {
      val file = new File(IOService.getChangeFile(id))
      file.length < 10000000
    })
    .foreach(id => {
      println(s"Starting $id")
      val cc = ChangeCube.load(id)
      val propertyMap = cc.colIDTOAttributeMap
        .map{case (k,map) => (k,map.last._2.name)}
        .toIndexedSeq
        .groupBy(_._2)
        .flatMap{case (n,attrs) => {
          if(attrs.size==1)
            attrs
          else {
            var count = 0
            attrs.sortBy(_._1)
              .map{case (i,s) => {
                val toReturn = (i,s+s"_$count")
                count+=1
                toReturn
              }}
          }
        }
        }
      cc.allChanges
        .withFilter(c => {
          val property = sanitizeValue(propertyMap(c.pID))
          c.value!=null && c.e!=null && property!=null
        })
        .foreach(c => {
        if(curLineCount==100000){
          curResultPR.close()
          curFilenameCounter +=1
          curResultPR = new PrintWriter(resultDir + "/part_" + curFilenameCounter + ".csv")
          curResultPR.println("Dataset_ID,Timestamp,EntityID,AttributeName,newValue")
          println(s"Done with $curLineCount rows")
          curLineCount=0
        }
        val property = sanitizeValue(propertyMap(c.pID))
        curResultPR.println(s"${sanitizeValue(id)},${sanitizeValue(Timestamp.valueOf(c.t.atStartOfDay()).toString)},${sanitizeValue(c.e.toString)},$property,${sanitizeValue(c.value.toString)}")
        curLineCount+=1
      })
    })
  curResultPR.close()

}
