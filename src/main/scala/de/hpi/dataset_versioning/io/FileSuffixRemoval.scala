package de.hpi.dataset_versioning.io

import java.io.File
import scala.sys.process._
import scala.language.postfixOps

object FileSuffixRemoval extends App {
  val dir = new File(args(0))
  val suffixToDelete = args(1)
  val files = dir.listFiles()
    .filter(_.getName.endsWith(suffixToDelete))
  files.foreach(f => {
    val targetPath = f.getParent + "/" +f.getName.replace(suffixToDelete,"")
    val toExecute = s"mv ${f.getAbsolutePath} ${targetPath}"
    toExecute!
  })
}
