package de.hpi.oneshot

import scala.io.Source

object TestReadMain extends App {
  val it = Source.fromFile("test.txt")
    .getLines()
  while (it.hasNext)
    println(it.next())
}
