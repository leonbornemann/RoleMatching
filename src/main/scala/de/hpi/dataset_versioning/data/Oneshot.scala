package de.hpi.dataset_versioning.data

import scala.io.Source

object Oneshot extends App {
  val a = Source.fromFile("/home/leon/data/dataset_versioning/socrata/R_scripts_and_data/subdomainSize.txt")
    .getLines()
    .toSeq
    .map(l => l.split("\\s+"))
    .map(t => {
      if(t.size!=2)
        assert(false)
      (t(1).split("_")(0),t(0).toInt)
    })
    .sortBy(-_._2)
    .foreach{case (v,w) => println(s"$v $w")}

}
