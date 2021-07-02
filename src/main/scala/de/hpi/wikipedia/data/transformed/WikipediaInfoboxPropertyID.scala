package de.hpi.wikipedia.data.transformed

import de.hpi.wikipedia.data.transformed.WikipediaInfoboxPropertyID.separator

import java.util.regex.Pattern

case class WikipediaInfoboxPropertyID(template: Option[String], pageID: BigInt, key: String, p: String) {

  def toCompositeID: String = Seq(template.getOrElse(""),pageID,key,p).mkString(separator)

  def toWikipediaURLInfo = s"https://en.wikipedia.org/?curid=$pageID ($p)"
}
object WikipediaInfoboxPropertyID {
  def from(str: String) = {
    if(str.startsWith(separator)){
      val tokens = str.substring(separator.length).split(Pattern.quote(separator))
      WikipediaInfoboxPropertyID(None,BigInt(tokens(0)),tokens(1),tokens(2))
    } else {
      val tokens = str.split(Pattern.quote(separator))
      WikipediaInfoboxPropertyID(Some(tokens(0)),BigInt(tokens(1)),tokens(2),tokens(3))
    }
  }

  def separator = "||"

}
