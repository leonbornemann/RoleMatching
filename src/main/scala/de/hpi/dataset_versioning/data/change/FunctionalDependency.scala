package de.hpi.dataset_versioning.data.change

import java.io.File

import scala.io.Source

case class FunctionalDependency(left:Seq[String],right:Seq[String])

object FunctionalDependency {

  def fromMetanomeOutputFile(fdFile: File) = {
    Source.fromFile(fdFile)
      .getLines()
      .toSeq
      .map(s => {
        val tokens = s.split("-->")
        assert(tokens.size==2)
        val leftList = tokens(0).trim
        assert(leftList.endsWith("]") && leftList.startsWith("["))
        val left = leftList.substring(1,leftList.size-1)
          .split(",")
          .map(_.trim)
        val right = tokens(1)
          .split(",")
          .map(_.trim)
        FunctionalDependency(left,right)
      })
  }

}
