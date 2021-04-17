package de.hpi.tfm.data.wikipedia.infobox

import java.io.{ByteArrayOutputStream, File, FileOutputStream}

object TimeBordersPrintMain extends App {
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
  new File(tmpOutputFile).delete()
}
