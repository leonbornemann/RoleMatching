package de.hpi.util

import scala.io.Source

/***
 * Reads the output log file of the linux parallel command and can be used to search for processes that did not terminate successfully
 */
object ReadNonZeroExitValIDsMain extends App {

  val file = args(0)
  val idPositionInCommand = args(1).toInt
  val a = Source.fromFile(file)
    .getLines()
    .toSeq
    .tail
    .map(_.split("\t"))
    .filter(l => l(6).toInt != 0)
    .foreach(a => println(getID(a(8))))

  def getID(str: String) = {
    str.split("\\s")(idPositionInCommand)
  }

}
