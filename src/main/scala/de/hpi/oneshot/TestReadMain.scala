package de.hpi.oneshot

import de.hpi.role_matching.cbrm.data.Roleset

import scala.io.Source

object TestReadMain extends App {
  val rolesetFile = "/home/leon/data/dataset_versioning/finalExperiments/nodecayrolesets_wikipedia/football.json"
  val rs = Roleset.fromJsonFile(rolesetFile)
  rs.getStringToLineageMap
    .values
    .filter(_.id.contains("infobox stadium||891011||138090886-0||tenants_"))
    .foreach(l => println(l.id))
  val it = Source.fromFile("test.txt")
    .getLines()
  while (it.hasNext)
    println(it.next())
}
