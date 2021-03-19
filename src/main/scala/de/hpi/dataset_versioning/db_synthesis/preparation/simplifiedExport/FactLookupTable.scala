package de.hpi.dataset_versioning.db_synthesis.preparation.simplifiedExport

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

import java.io.File

case class FactLookupTable(id: DecomposedTemporalTableIdentifier, factTableRows: IndexedSeq[FactTableRow], surrogateKeyToVL: IndexedSeq[(Int, ValueLineage)]) extends JsonWritable[FactLookupTable] {

  def writeToStandardFile() = {
    val file = FactLookupTable.getStandardFile(id)
    toJsonFile(file)
   }


}
object FactLookupTable extends JsonReadable[FactLookupTable] {

  def readFromStandardFile(id:DecomposedTemporalTableIdentifier) = fromJsonFile(getStandardFile(id).getAbsolutePath)

  def getStandardFile(id: DecomposedTemporalTableIdentifier) = {
    val dir = DBSynthesis_IOService.OPTIMIZATION_INPUT_FACTLOOKUP_DIR(id.viewID)
    val file = DBSynthesis_IOService.createParentDirs(new File(dir + s"/${id.compositeID}.json"))
    file
  }

}
