package de.hpi.role_matching.data.json_serialization

import org.json4s.DefaultFormats

import java.io.{File, FileWriter, StringWriter}

trait JsonWritable[T <: AnyRef] {

  implicit def formats = (DefaultFormats.preservingEmptyValues
    + LocalDateSerializer
    + LocalDateKeySerializer)

  def toJson() = {
    org.json4s.jackson.Serialization.write(this)
  }

  def toJsonFile(file: File, pretty: Boolean = false) = {
    val writer = new FileWriter(file)
    org.json4s.jackson.Serialization.writePretty(this, writer)
    writer.close()
  }

  def appendToWriter(writer: java.io.Writer, pretty: Boolean = false, addLineBreak: Boolean = false, flush: Boolean = false) = {
    val writerS = new StringWriter()
    if (pretty)
      org.json4s.jackson.Serialization.writePretty(this, writerS)
    else
      org.json4s.jackson.Serialization.write(this, writerS)
    writer.append(writerS.toString)
    if (addLineBreak)
      writer.append("\n")
    if (flush)
      writer.flush()
  }
}
