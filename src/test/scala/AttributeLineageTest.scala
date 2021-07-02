import de.hpi.socrata.change.ChangeExporter
import de.hpi.socrata.change.temporal_tables.TemporalTable
import de.hpi.socrata.history.VersionHistoryConstruction
import de.hpi.socrata.io.Socrata_IOService

object AttributeLineageTest extends App {

  Socrata_IOService.socrataDir = "/home/leon/data/dataset_versioning/socrata/testDir/"
  val id1 = "AttributeLineageTest"
  val versionHistoryConstruction = new VersionHistoryConstruction()
  versionHistoryConstruction.constructVersionHistoryForSimplifiedFiles()
  new ChangeExporter().exportAllChanges(id1)
  val temporalTable = TemporalTable.load(id1)
  assert(temporalTable.attributes.exists(al => al.lineage.size==3 && al.lineage.toIndexedSeq(1)._2.isNE))
}
