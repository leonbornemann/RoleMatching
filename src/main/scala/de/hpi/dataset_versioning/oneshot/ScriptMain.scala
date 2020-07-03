package de.hpi.dataset_versioning.oneshot

import de.hpi.dataset_versioning.data.change.ChangeCube
import de.hpi.dataset_versioning.io.IOService

object ScriptMain extends App {
  println("\u2400")
  IOService.socrataDir = args(0)
  val cc = ChangeCube.load("5cq6-qygt")
  println(cc.deletes.size)
  println()
}
