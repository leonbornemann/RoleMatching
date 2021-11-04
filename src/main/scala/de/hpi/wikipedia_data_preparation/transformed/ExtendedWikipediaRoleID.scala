package de.hpi.wikipedia_data_preparation.transformed

import de.hpi.wikipedia_data_preparation.transformed.ExtendedWikipediaRoleID.separator

import java.util.regex.Pattern

case class ExtendedWikipediaRoleID(template: Option[String], pageID: BigInt, key: String, p: String) {

  def toCompositeID: String = Seq(template.getOrElse(""),pageID,key,p).mkString(separator)

  def toWikipediaURLInfo = s"https://en.wikipedia.org/?curid=$pageID ($p)"
}
object ExtendedWikipediaRoleID {
  def from(str: String) = {
    if(str.startsWith(separator)){
      val tokens = str.substring(separator.length).split(Pattern.quote(separator))
      ExtendedWikipediaRoleID(None,BigInt(tokens(0)),tokens(1),tokens(2))
    } else {
      val tokens = str.split(Pattern.quote(separator))
      ExtendedWikipediaRoleID(Some(tokens(0)),BigInt(tokens(1)),tokens(2),tokens(3))
    }
  }

  def separator = "||"

}
