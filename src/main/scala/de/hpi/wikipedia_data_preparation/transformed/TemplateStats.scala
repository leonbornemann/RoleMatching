package de.hpi.wikipedia_data_preparation.transformed

import de.hpi.role_matching.playground.{JsonReadable, JsonWritable}

import scala.collection.mutable

case class TemplateStats(nameToCount:collection.mutable.HashMap[String,Int]) extends JsonWritable[TemplateStats] {

  def addAll(retained: Iterable[WikipediaRoleLineage]) = {
    retained
      .groupBy(wrl => wrl.template.getOrElse(""))
      .withFilter(_._1 != "")
      .foreach{case (template,wrls) => {
        val old = nameToCount.getOrElse(template,0)
        nameToCount.put(template,old + wrls.size)
      }}
  }


}

object TemplateStats extends JsonReadable[TemplateStats]{

}
