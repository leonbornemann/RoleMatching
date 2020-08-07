package de.hpi.dataset_versioning.oneshot

import java.io.{File, FileReader}
import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.{ChangeCube, ChangeExporter, TemporalTable}
import de.hpi.dataset_versioning.data.simplified.RelationalDataset
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.fd.FunctionalDependencySet
import de.hpi.dataset_versioning.io.IOService
import org.apache.commons.csv.{CSVFormat, CSVParser}

import scala.collection.mutable
import scala.io.Source

object ScriptMain extends App {
  val lines = Source.fromFile("/home/leon/Desktop/statsNew.csv")
    .getLines()
    .toSeq
    .tail
    .map(_.split(",").map(_.toInt).toIndexedSeq)
    .filter(_(0)!=0)
  println(lines)
  val fdIntersectionSelectivity = lines.map(l => l(1) / l(0).toDouble)
  var relativeOverlap = lines.filter(l => l(0) != l(1)).map(l => l(4) / l(2).toDouble)
  relativeOverlap = relativeOverlap ++ (0 until 42).map(_ => 1.0)
  val avgOverlap = relativeOverlap.sum / relativeOverlap.size.toDouble
  println(avgOverlap)
  var fdIntersectionSelectivityWithoutEquality = lines.filter(l => l(0) != l(1)).map(l => l(1) / l(0).toDouble)
  //account for thos with changes that are equal
  fdIntersectionSelectivityWithoutEquality= fdIntersectionSelectivityWithoutEquality ++ (0 until 42).map(_ => 1.0)

  println(fdIntersectionSelectivity.sum / fdIntersectionSelectivity.size.toDouble)
  println(fdIntersectionSelectivity.size)
  println(fdIntersectionSelectivityWithoutEquality.sum / fdIntersectionSelectivityWithoutEquality.size.toDouble)
  println(fdIntersectionSelectivityWithoutEquality.size)

  println(
  )
  fdIntersectionSelectivityWithoutEquality.foreach(println(_))
}
