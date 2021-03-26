package de.hpi.tfm.io

import java.io.File
import java.nio.file.Files
import java.util

object IOUtil {
  def clearDirectoryContent(dir: File) = dir.listFiles().foreach(_.delete())

  def dirEquals(dir1:File,dir2:File) = {
    val dir1Names = dir1.listFiles().map(_.getName).toSet
    val dir2Names = dir1.listFiles().map(_.getName).toSet
    if (!dir1Names.equals(dir2Names)) {
      false
    } else {
      val iterator = dir1Names.iterator
      var equal = true
      while (equal && iterator.hasNext) {
        val curName = iterator.next()
        val dir1File = new File(dir1.getAbsolutePath + "/" + curName)
        val dir2File = new File(dir2.getAbsolutePath + "/" + curName)
        val bytes1 = Files.readAllBytes(dir1File.toPath)
        val bytes2 = Files.readAllBytes(dir2File.toPath)
        if (!util.Arrays.equals(bytes1, bytes2)) {
          equal = false
        }
      }
      equal
    }
  }
}
