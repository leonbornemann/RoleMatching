package de.hpi.oneshot

import scala.io.Source

object TemplateAnalysis extends App {
  val allTemplateNamesIntitially = Source.fromFile("src/main/resources/tmp/top100Templates.txt").getLines().toSet
  val templateStats = Source.fromFile("src/main/resources/tmp/templateStats.txt").getLines().toIndexedSeq
  val indices = templateStats.zipWithIndex.filter(!_._1.contains(":")).map(_._2)
    .zipWithIndex
  val templates = indices.map{case (i,indexOfI) => {
    val end = if(indexOfI==indices.size-1) templateStats.size else indices(indexOfI+1)._1
    val curTemplates = templateStats.slice(i+1,end).map(_.split(" : ")(0)).take(100).toSet
    (templateStats(i),curTemplates)
  }}.toMap
  templates.foreach{ case (k,t) => {
    println(s"$k ${t.intersect(allTemplateNamesIntitially).size}")
  }}
  for(t <- templates){
    for (t1 <- templates){
      if(t._1!=t1._1)
        println(s"${t._1} ${t1._1} ${t._2.intersect(t1._2).size}")
    }
  }
  println(allTemplateNamesIntitially.diff(templates.head._2))
}
