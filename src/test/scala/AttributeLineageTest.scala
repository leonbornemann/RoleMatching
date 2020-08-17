import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.change.{ChangeExportMain, ChangeExporter}
import de.hpi.dataset_versioning.data.history.{DatasetVersionHistory, VersionHistoryConstruction}
import de.hpi.dataset_versioning.io.IOService

object AttributeLineageTest extends App {

  IOService.socrataDir = "/home/leon/data/dataset_versioning/socrata/testDir/"
  val id1 = "AttributeLineageTest"
  val versionHistoryConstruction = new VersionHistoryConstruction()
  versionHistoryConstruction.constructVersionHistoryForSimplifiedFiles()
  new ChangeExporter().exportAllChanges(id1,false)
  val temporalTable = TemporalTable.load(id1)
  println()
  assert(temporalTable.attributes.exists(al => al.lineage.size==3 && al.lineage.toIndexedSeq(1)._2.isNE))
}
