package de.hpi.role_matching.blocking

import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdgeID

import java.io.File

class ComputeBlockingSizeForFullCompatibilityMain extends App {
  val edgeDir = new File(args(0))
  println("dataset","edgeCount")
  edgeDir.listFiles().foreach(f => {
    val dirPath = new File(f.getAbsolutePath + "/edges/")
    if(dirPath.exists()) {
      val edgeCount = SimpleCompatbilityGraphEdgeID.iterableFromJsonObjectPerLineDir(dirPath).size
      println(f.getName,",",edgeCount)
    }
  })
}
