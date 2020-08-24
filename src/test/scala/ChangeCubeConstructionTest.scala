import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.{ChangeCube, ChangeExporter, ReservedChangeValues}
import de.hpi.dataset_versioning.io.IOService

object ChangeCubeConstructionTest extends App {

  def testChangeCubeCreation() = {
    val exported = new ChangeExporter
    val versions = IndexedSeq(LocalDate.parse("2019-11-01"),LocalDate.parse("2019-11-02"),LocalDate.parse("2019-11-03"),LocalDate.parse("2019-11-04"),LocalDate.parse("2019-11-05"),LocalDate.parse("2019-11-06"))
    exported.exportAllChangesFromVersions(id,versions)
    val changesLoaded = ChangeCube.load(id)
    assert(changesLoaded.allChanges.filter(_.t==LocalDate.parse("2019-11-03"))
      .forall(_.value==ReservedChangeValues.NOT_EXISTANT_DATASET))
    //do assertions here:
    changesLoaded.allChanges.filter(c => c.t==versions(1) && c.e==120).foreach(c => {
      if(c.pID!=10)
        assert(c.value==ReservedChangeValues.NOT_EXISTANT_ROW)
      else
        assert(c.value==ReservedChangeValues.NOT_EXISTANT_COL)
    })
    changesLoaded.allChanges.filter(c => c.e ==120 && c.t == versions(3))
      .foreach(c => {
        if(c.pID!=10)
          assert(c.value==ReservedChangeValues.NOT_EXISTANT_ROW)
        else
          assert(c.value==ReservedChangeValues.NOT_EXISTANT_COL)
      })
    assert({
      val curChanges = changesLoaded.allChanges.filter(c => c.e == 120 && c.t == versions(2))
      curChanges.forall(c => c.value == ReservedChangeValues.NOT_EXISTANT_DATASET) && !curChanges.isEmpty
    })
    //TODO: also add a fix that changes from row deleted to dataset deleted to column deleted to row deleted
  }

  IOService.socrataDir = "/home/leon/data/dataset_versioning/socrata/testDir/"
  var id = "change-cube-creation-test"
  testChangeCubeCreation()
  id = "change-cube-creation-test-deletes-correct"
  testCorrectWildcardCreation()
  id = "change-cube-creation-non-present-before-initial-observation-correct"

  def testCorrectAbsentValuesBeforeIntialEntityCreation() = {
    val exported = new ChangeExporter
    val versions = IndexedSeq(
      LocalDate.parse("2019-11-02"),
      LocalDate.parse("2019-11-03"))
    exported.exportAllChangesFromVersions(id,versions)
    val changesLoaded = ChangeCube.load(id)
    val firstAbsentTs = changesLoaded.allChanges.filter(_.t==LocalDate.parse("2019-11-01"))
    assert(!firstAbsentTs.isEmpty)
    firstAbsentTs.foreach(c =>
      assert(c.value == ReservedChangeValues.NOT_EXISTANT_DATASET)
    )
    val secondTimestampRow120 = changesLoaded.allChanges.filter(_.t==versions(0)).filter(_.e==120)
    assert(!secondTimestampRow120.isEmpty)
    secondTimestampRow120.foreach(c => {
      if(c.pID==11) {
        assert(c.value==ReservedChangeValues.NOT_EXISTANT_COL)
      } else
        assert(c.value==ReservedChangeValues.NOT_EXISTANT_ROW)
    })
  }

  testCorrectAbsentValuesBeforeIntialEntityCreation()

  def testCorrectWildcardCreation() = {
    val exported = new ChangeExporter
    val versions = IndexedSeq(LocalDate.parse("2019-11-01"),
      LocalDate.parse("2019-11-02"),
      LocalDate.parse("2019-11-03"),
      LocalDate.parse("2019-11-04"),
      LocalDate.parse("2019-11-05"),
      LocalDate.parse("2019-11-06"),
      LocalDate.parse("2019-11-07"),
      LocalDate.parse("2019-11-08"))
    exported.exportAllChangesFromVersions(id,versions)
    val changesLoaded = ChangeCube.load(id)
    assert(changesLoaded.allChanges.filter(_.t==versions(1))
      .forall(_.value==ReservedChangeValues.NOT_EXISTANT_DATASET))
    var changesToCheck = changesLoaded.allChanges.filter(c => c.t == versions(2) && c.e == 120)
    assert(!changesToCheck.isEmpty)
    changesToCheck.foreach(c => {
      if(!Seq(10,13,14).contains(c.pID)) {
        assert(c.value==ReservedChangeValues.NOT_EXISTANT_ROW)
      } else
        assert(c.value==ReservedChangeValues.NOT_EXISTANT_COL)
    })
    changesToCheck = changesLoaded.allChanges.filter(c => c.t==versions(3) && c.e==120)
    assert(!changesToCheck.isEmpty)
    changesToCheck.foreach(c => {
      if(c.pID!=10 && c.pID!=9)
        assert(c.value==ReservedChangeValues.NOT_EXISTANT_ROW)
      else
        assert(c.value==ReservedChangeValues.NOT_EXISTANT_COL)
    })
    changesToCheck = changesLoaded.allChanges.filter(c => c.t==versions(5) && c.e==120)
    assert(!changesToCheck.isEmpty)
    changesToCheck.foreach(c => {
      if(!Seq(13,14).contains(c.pID)) {
        assert(c.value==ReservedChangeValues.NOT_EXISTANT_ROW)
      } else
        assert(c.value==ReservedChangeValues.NOT_EXISTANT_COL)
    })
    changesToCheck = changesLoaded.allChanges.filter(c => c.t == versions(7) && c.e==120 && (c.pID == 0 || c.pID==1) )
    assert(changesToCheck.forall(_.value == ReservedChangeValues.NOT_EXISTANT_COL))
  }


}
