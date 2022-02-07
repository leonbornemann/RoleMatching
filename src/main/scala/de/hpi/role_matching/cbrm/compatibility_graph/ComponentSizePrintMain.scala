package de.hpi.role_matching.cbrm.compatibility_graph

import de.hpi.role_matching.cbrm.sgcp.Histogram

import java.io.File

object ComponentSizePrintMain extends App {
  val dir = new File(args(0))
  dir.listFiles().foreach(f => {
    println(s"${f.getName}--------------------------------------------------------------------------------------")
    val hist = Histogram.fromJsonFile(f.getAbsolutePath)
    hist.printAll()
    println(s"-----------------------------------------------------------------------------------------------------------")
  })
}
