package de.hpi.dataset_versioning.oneshot

import java.io.PrintWriter

import scala.io.Source

object ColumnAndRowMatchingExitValRead extends App {

  val a = Source.fromFile("/home/leon/Desktop/log")
    .getLines()
    .toSeq
    val grouped = a.tail
    .map(_.split("\t"))
    .groupBy(l => l(6).toInt)
    .filter(_._1!=0)

  def getID(str: String) = {
    str.split("\\s")(8)
  }
  val pr = new PrintWriter("socrataIdsUnfinished.txt")
  grouped.foreach{case (exit,lines) => {
    if(exit!=1)
      lines.foreach(l => pr.println(getID(l(8))))
    println(exit,lines.size,lines.map(l => getID(l(8))).sorted.mkString(","))
  }}
  pr.close()
}
