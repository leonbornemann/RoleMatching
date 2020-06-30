import java.io.File
import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.DiffAsChangeCube
import de.hpi.dataset_versioning.data.simplified.{Attribute, RelationalDataset}
import de.hpi.dataset_versioning.db_synthesis.top_down.decomposition.normalization.DecomposedTable
import de.hpi.dataset_versioning.db_synthesis.top_down.merge.measures.InferredChangeCountReduction
import de.hpi.dataset_versioning.io.IOService

object InferredChangeCountReductionTest extends App {
  IOService.socrataDir = args(0)
  val id1 = "test-merge-benefit-1"
  val id2 = "test-merge-benefit-2"
  val version0 = LocalDate.parse("2019-11-01",IOService.dateTimeFormatter)
  val version1 = LocalDate.parse("2019-11-02",IOService.dateTimeFormatter)
  val ds1 = RelationalDataset.load(id1,version1)
  val ds2 = RelationalDataset.load(id2,version1)
  val ds1_prev = RelationalDataset.load(id1,version0)
  val ds2_prev = RelationalDataset.load(id2,version0)
  DiffAsChangeCube.fromDatasetVersions(ds1_prev,ds1)
    .changeCube
    .addAll(DiffAsChangeCube.fromDatasetVersions(RelationalDataset.createEmpty(id1,version0),ds1_prev).changeCube)
    .toJsonFile(new File(IOService.getChangeFile(id1)))
  DiffAsChangeCube.fromDatasetVersions(ds2_prev,ds2)
    .changeCube
    .addAll(DiffAsChangeCube.fromDatasetVersions(RelationalDataset.createEmpty(id2,version0),ds2_prev).changeCube)
    .toJsonFile(new File(IOService.getChangeFile(id2)))
  val counter = new InferredChangeCountReduction()
  val attrsDecomposed1 = ds1.attributes.takeRight(2)
  val attrsDecomposed2 = ds2.attributes.takeRight(2)
  val decomposedTable1 = new DecomposedTable(id1,version1,0,attrsDecomposed1,Set("C"),Set())
  val decomposedTable2 = new DecomposedTable(id2,version1,0,attrsDecomposed2,Set("C"),Set())
  val reducedNumChanges = counter.calculate(decomposedTable1,decomposedTable2,attrsDecomposed1.zip(attrsDecomposed2).toMap)
  assert(reducedNumChanges==3)
}
