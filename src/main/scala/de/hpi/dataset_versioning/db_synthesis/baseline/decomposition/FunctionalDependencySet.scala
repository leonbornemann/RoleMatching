package de.hpi.dataset_versioning.db_synthesis.baseline.decomposition

import java.time.LocalDate

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

case class FunctionalDependencySet(subdomain:String,id:String,curDate: LocalDate, fds: collection.IndexedSeq[(collection.Set[Int], collection.Set[Int])]) extends JsonWritable[FunctionalDependencySet]{

  def writeToStandardFile() = {
    val file = DBSynthesis_IOService.getColIDFDFile(subdomain,id,curDate)
    toJsonFile(file)
  }
}
object FunctionalDependencySet extends JsonReadable[FunctionalDependencySet] {
  def load(subdomain: String, id: String, date: LocalDate) = fromJsonFile(DBSynthesis_IOService.getColIDFDFile(subdomain,id,date).getAbsolutePath)

}
