package de.hpi.tfm.data.wikipedia.infobox

import java.io.{ByteArrayOutputStream, File, FileOutputStream}
import java.time.LocalDate
import scala.io.Source

object TimeBordersPrintMain extends App {
//  val fileMin = "/home/leon/data/dataset_versioning/WIkipedia/infoboxes/minTs.txt"
//  val fileMax = "/home/leon/data/dataset_versioning/WIkipedia/infoboxes/maxTs.txt"
//  val min = Source.fromFile(fileMin)
//    .getLines()
//    .toIndexedSeq
//    .map(s => LocalDate.parse(s))
//    .minBy(_.toEpochDay)
//  println(min)
//  val max = Source.fromFile(fileMax)
//    .getLines()
//    .toIndexedSeq
//    .map(s => LocalDate.parse(s))
//    .maxBy(_.toEpochDay)
//  println(max)
  //val archiveFile = new SevenZFile(new File(args(0)))
  val tmpOutputFile = args(0)
//  var entry:SevenZArchiveEntry = archiveFile.getNextEntry
//  val name = entry.getName
//  assert(!entry.isDirectory)
//  System.out.println(String.format("Unpacking %s ...", name))
//  val contentBytes = new ByteArrayOutputStream()
//  // ... using a small buffer byte array.
//  val buffer = new Array[Byte](2048)
//  var bytesRead = 0
//  while ( {
//    (bytesRead = archiveFile.read(buffer)) != -1
//  }) contentBytes.write(buffer, 0, bytesRead)
//  val outputStream = new FileOutputStream(tmpOutputFile)
//  contentBytes.writeTo(outputStream)
//  outputStream.close()
//  archiveFile.close()

  val dates = InfoboxRevision.fromJsonObjectPerLineFile(tmpOutputFile)
    .map(_.validFromAsDate.toLocalDate)
  println("min TImestamp: " + dates.minBy(_.toEpochDay))
  println("max: TImestamp" + dates.maxBy(_.toEpochDay))
  //new File(tmpOutputFile).delete()
}
