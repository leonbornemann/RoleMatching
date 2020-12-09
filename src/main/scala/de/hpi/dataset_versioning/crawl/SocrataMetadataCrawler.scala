package de.hpi.dataset_versioning.crawl

import com.google.gson.stream.JsonReader
import com.google.gson.{JsonArray, JsonParser}
import com.typesafe.scalalogging.StrictLogging

import java.io.{PrintWriter, StringReader}
import java.net.URL
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class SocrataMetadataCrawler(metadataResultDir:String) extends StrictLogging{

  val middleI = 5000

  def saveURLS(urls: ArrayBuffer[String], urlWriter:mutable.HashMap[String,mutable.ArrayBuffer[String]]) = {
    urls.foreach( url => {
      val domains = url.split("https://")(1).split("/resource/")(0).split("\\.").reverse.toSeq
      val topLvlDomains = if (domains.size>=2) domains.slice(0,2).mkString(".") else domains(0)
      val buffer = urlWriter.getOrElseUpdate(topLvlDomains,mutable.ArrayBuffer[String]())
      buffer += url
    })
  }

  def extractLastAssetID(resultsArray: JsonArray): String = {
    resultsArray.get(resultsArray.size()-1).getAsJsonObject
      .getAsJsonObject("resource")
      .get("id").getAsString
  }

  private def getResultsFromURL(url1: String) = {
    val res = Source.fromURL(url1).mkString
    //parse json array:
    val reader = new JsonReader(new StringReader(res))
    val parser = new JsonParser();
    var curObj = parser.parse(reader)
    val resultsArray = curObj.getAsJsonObject.getAsJsonArray("results")
    resultsArray
  }

  def crawl(urlDir:String) = {
    val limit = 10000
    var done = false
    var batchID = 0
    val topDomainsToURLS = new mutable.HashMap[String,mutable.ArrayBuffer[String]]()
    //224n-rp2d
    var lastAssetID = "0000-0000"
    //var lastAssetID = "224n-rp2d"
    //var lastAssetID = "4v23-4444"
    //var lastAssetID = "f2xe-wr7z"//getFirstScrollID() //todo:try different seed scroll-ids
    println(lastAssetID)
    logger.trace("Waiting 10 seconds to be polite")
    //Thread.sleep(10000)
    while(done!=true) {
      //fetch from URL:
      logger.trace("Fetching batch {}", batchID)
      val url = s"https://api.us.socrata.com/api/catalog/v1?only=dataset&limit=$limit&scroll_id=$lastAssetID"
      logger.trace(s"requesting $url")
      val resultsArray: JsonArray = getResultsFromURL(url)
      logger.trace("Processing {} dataset metadata objects",{resultsArray.size()})
      if (resultsArray.size() != 0) {
        //save metadata and extract content:
        val ids = extractAssetIds(resultsArray)
        addRequestResult(batchID, topDomainsToURLS, resultsArray)
        lastAssetID = extractLastAssetID(resultsArray)
        if (resultsArray.size() < limit) {
          done = true
        }
        batchID += 1
        logger.trace("Waiting 10 seconds to be polite")
        Thread.sleep(10000)
      } else {
        done = true
      }
    }
    println(topDomainsToURLS.values.map(_.toSet.size).sum)
    topDomainsToURLS.foreach{case (domain,urls) => {
      logger.trace(s"saving URLs for $domain")
      logger.trace(s"num urls: ${urls.size}, distinct: ${urls.toSet.size}")
      val pr = new PrintWriter(urlDir + "/" + domain + "_urls.txt")
      urls.toSet.foreach((url:String) => pr.println(url))
      pr.close()
    }}
  }

  private def addRequestResult(batchID: Int, topDomainsToURLS: mutable.HashMap[String, ArrayBuffer[String]], resultsArray: JsonArray) = {
    saveMetadata(resultsArray, batchID)
    val urls = extractDatasetURLS(resultsArray)
    saveURLS(urls, topDomainsToURLS)
  }

  def saveMetadata(curResult: JsonArray, batchID:Int) = {
    val outFile = metadataResultDir + "/batch_" + batchID + "_metadata.json"
    val pr = new PrintWriter(outFile)
    pr.println(curResult)
    pr.close()
  }

  def extractAssetIds(resultsArrray:JsonArray) = {
    val ids = mutable.ArrayBuffer[String]()
    for( i <-0 until resultsArrray.size()) {
      ids += resultsArrray.get(i).getAsJsonObject
        .getAsJsonObject("resource")
        .get("id").getAsString
    }
    ids
  }

  def extractDatasetURLS(resultsArray:JsonArray) = {
    val nonCompliantURLs = mutable.ArrayBuffer[String]()
    val compliantURLs = mutable.ArrayBuffer[String]()
    (0 until resultsArray.size()).map(i => {
      val curResult = resultsArray.get(i).getAsJsonObject
      //save metadata:
      val l = curResult.get("permalink").getAsString
      val parts = new URL(l).getPath.split("/")
      if(parts(1)!="d" || parts.zipWithIndex.exists{case (p,i) => i!=1 && p == "d"}){
        nonCompliantURLs += l
      } else{
        compliantURLs += l
      }
    })
    val transformedURLs = compliantURLs.map(url => url.replace("/d/","/resource/") + ".json?")
    logger.trace(s"compliantURLs:${compliantURLs.size}")
    logger.trace(s"nonCompliantURLs:${nonCompliantURLs.size}")
    transformedURLs
  }

}
