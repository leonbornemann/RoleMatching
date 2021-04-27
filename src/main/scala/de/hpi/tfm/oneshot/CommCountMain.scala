package de.hpi.tfm.oneshot

import scala.io.Source

object CommCountMain extends App {
  Source.fromFile("/home/leon/Desktop/Impf E-Mails")
    .getLines()
    .toIndexedSeq
    .mkString(" ")
    .split(" ")
    .filter(_.contains("@"))
    .map(_.trim)
    .foreach(println)
    //.foreach(l => sizes += l.filter(_ == ',').size)
//  var sizes = scala.collection.mutable.HashSet[Int]()
//  Source.fromFile("/home/leon/data/dataset_versioning/plotting/data/allWikipediaStats.csv")
//    .getLines()
//    .foreach(l => sizes += l.filter(_ == ',').size)
//  println(sizes.toIndexedSeq.sorted)

}
