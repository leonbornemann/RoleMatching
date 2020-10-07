import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.io.IOService

object ChangeCounterTest extends App {

  IOService.socrataDir = "/home/leon/data/dataset_versioning/socrata/fromServer"
  println("binary")
  var start = System.currentTimeMillis()
  var tt = TemporalTable.load("pubx-yq2d")
  var end = System.currentTimeMillis()
  println(s"Binary Took ${end-start}ms")
  start = System.currentTimeMillis()
  tt = TemporalTable.loadFromChangeCube("pubx-yq2d")
  end = System.currentTimeMillis()
  println(s"Json Took ${end-start}ms")
  val res = tt.countChanges(GLOBAL_CONFIG.CHANGE_COUNT_METHOD,Set())
  println(res)
}
