package de.hpi.oneshot

import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdgeID

import java.io.File
import scala.io.Source

object ResultComparisonMain extends App {
  val dir1 = "/home/leon/data/dataset_versioning/finalExperiments/scalabilityImprovement/oldVersion/Edges_n2/"
  val dir2 = "/home/leon/data/dataset_versioning/finalExperiments/scalabilityImprovement/newVersion2/EdgesOregon_n2/"
  val edgesOld = SimpleCompatbilityGraphEdgeID.iterableFromJsonObjectPerLineDir(new File(dir1))
    .toSet
  val edgesNew = SimpleCompatbilityGraphEdgeID.iterableFromJsonObjectPerLineDir(new File(dir2)).toSet
  println(edgesOld.intersect(edgesNew).size)
  edgesOld.diff(edgesNew).foreach(println)
  println("----------------------------------------------------------------------------------")
  println(edgesNew.diff(edgesOld).foreach(println))
  //println(edgesOld.diff(edgesNew).size)
  //val notInNew = edgesOld.diff(edgesNew).take(10).foreach(println)

}
