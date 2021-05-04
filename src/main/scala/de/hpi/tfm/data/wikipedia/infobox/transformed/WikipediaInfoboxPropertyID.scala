package de.hpi.tfm.data.wikipedia.infobox.transformed

import de.hpi.tfm.data.wikipedia.infobox.transformed.WikipediaInfoboxPropertyID.separator

case class WikipediaInfoboxPropertyID(template: Option[String], pageID: BigInt, key: String, p: String) {

  def toCompositeID: String = Seq(template.getOrElse(""),pageID,key,p).mkString(separator)

}
object WikipediaInfoboxPropertyID {
  def separator = "||"

}
