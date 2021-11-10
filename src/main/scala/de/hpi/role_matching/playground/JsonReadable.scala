package de.hpi.role_matching.playground

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import java.io.{File, FileInputStream}
import scala.io.Source


trait JsonReadable[T <: AnyRef] {

  implicit def formats = (DefaultFormats.preservingEmptyValues)


  def fromJsonString(json: String)(implicit m: Manifest[T]) = {
    parse(json).extract[T]
  }

  def fromJsonFile(path: String)(implicit m: Manifest[T]) = {
    //val string = Source.fromFile(path).getLines().mkString("\n")
    val file = new FileInputStream(new File(path))
    val json = parse(file)
    json.extract[T]
  }

  def iterableFromJsonObjectPerLineFile(path: String)(implicit m: Manifest[T]) = {
    new JsonObjectPerLineFileIterator(path)(m)
  }

  def iterableFromJsonObjectPerLineDir(dir: File)(implicit m: Manifest[T]) = {
    val iterators = dir.listFiles().toIndexedSeq
      .map(f => iterableFromJsonObjectPerLineFile(f.getAbsolutePath))
    iterators.foldLeft(Iterator[T]())(_ ++ _)
  }

  def fromJsonObjectPerLineFile(path: String)(implicit m: Manifest[T]): collection.Seq[T] = {
    val result = scala.collection.mutable.ArrayBuffer[T]()
    Source.fromFile(path).getLines()
      .foreach(l => {
        result.addOne(fromJsonString(l))
      })
    result
  }

  class JsonObjectPerLineFileIterator(path: String)(implicit m: Manifest[T]) extends Iterator[T] {
    val it = Source.fromFile(path).getLines()

    override def hasNext: Boolean = it.hasNext

    override def next(): T = fromJsonString(it.next())
  }
}
