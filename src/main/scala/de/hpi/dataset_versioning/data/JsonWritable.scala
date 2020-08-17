package de.hpi.dataset_versioning.data

import java.io.{File, FileWriter, StringWriter}

import de.hpi.dataset_versioning.data.change.ReservedChangeValues
import de.hpi.dataset_versioning.data.json.custom_serializer.{DatasetInstanceKeySerializer, LocalDateKeySerializer, LocalDateSerializer}
import de.hpi.dataset_versioning.data.metadata.Provenance
import de.hpi.dataset_versioning.data.metadata.custom.ColumnDatatype
import org.json4s.DefaultFormats
import org.json4s.ext.EnumNameSerializer

trait JsonWritable[T<:AnyRef] {

  implicit val formats = (DefaultFormats.preservingEmptyValues
    + new EnumNameSerializer(Provenance)
    + new EnumNameSerializer(ColumnDatatype)
    + LocalDateSerializer
    + DatasetInstanceKeySerializer
    + LocalDateKeySerializer)

  def toJson() = {
    org.json4s.jackson.Serialization.write(this)
  }

  def toJsonFile(file:File,pretty:Boolean=false) = {
    val writer = new FileWriter(file)
    org.json4s.jackson.Serialization.writePretty(this,writer)
    writer.close()
  }

  def appendToWriter(writer:java.io.Writer,pretty:Boolean=false,addLineBreak:Boolean=false,flush:Boolean=false) = {
    val writerS = new StringWriter()
    if(pretty)
      org.json4s.jackson.Serialization.writePretty(this,writerS)
    else
      org.json4s.jackson.Serialization.write(this,writerS)
    writer.append(writerS.toString)
    if(addLineBreak)
      writer.append("\n")
    if(flush)
      writer.flush()
  }
}
