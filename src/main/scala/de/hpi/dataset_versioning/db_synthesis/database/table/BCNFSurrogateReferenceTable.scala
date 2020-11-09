package de.hpi.dataset_versioning.db_synthesis.database.table

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.data.change.temporal_tables.SurrogateAttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.sketches.{BinaryReadable, BinarySerializable}
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

@SerialVersionUID(3L)
class BCNFSurrogateReferenceTable(val bcnfTableSchema: BCNFTableSchema,
                                  val associationReferences:collection.IndexedSeq[SurrogateAttributeLineage],
                                  val rows:IndexedSeq[BCNFSurrogateReferenceRow]) extends JsonWritable[BCNFSurrogateReferenceTable]{

  override def toString: String = bcnfTableSchema.toString

  def writeToStandardOptimizationInputFile = {
    val file = DBSynthesis_IOService.getOptimizationBCNFReferenceTableInputFile(bcnfTableSchema.id)
    toJsonFile(file)
  }

}
object BCNFSurrogateReferenceTable extends JsonReadable[BCNFSurrogateReferenceTable] {
  def loadFromStandardOptimizationInputFile(id:DecomposedTemporalTableIdentifier) = {
    val file = DBSynthesis_IOService.getOptimizationBCNFReferenceTableInputFile(id)
    fromJsonFile(file.getAbsolutePath)
  }
}
