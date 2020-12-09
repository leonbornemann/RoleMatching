import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.ChangeExporter
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.TopDownOptimizer
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.natural_key_based.DecomposedTemporalTable
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

object FullBaselinePipelineTest extends App {
  IOService.socrataDir = "/home/leon/data/dataset_versioning/socrata/testDir/"
  val versions = IndexedSeq(LocalDate.parse("2019-11-01"), LocalDate.parse("2019-11-02"))
  val idA = "fullBaselineTest-A"
  val idB = "fullBaselineTest-B"
  val idC = "fullBaselineTest-C"
  val idD = "fullBaselineTest-D"
  val idSplitPK = "fullBaselineTest-SplitPK"
  val idNormalPK = "fullBaselineTest-NormalPK"
  val exporter = new ChangeExporter
  val allViewIds = Seq(idA,idB,idC,idD,idSplitPK,idNormalPK)
  allViewIds.foreach(exporter.exportAllChangesFromVersions(_,versions))
  //how to proceed?
  val ttA = TemporalTable.load(idA)
  val ttB = TemporalTable.load(idB)
  val ttC = TemporalTable.load(idC)
  val ttD = TemporalTable.load(idD)
  val ttSplitPK = TemporalTable.load(idSplitPK)
  val ttNormalPk = TemporalTable.load(idNormalPK)
  //A
  val pkA1 = ttA.attributes.filter(al => Seq(0).contains(al.attrId)).toSet
  val pkA2 = ttA.attributes.filter(al => Seq(2).contains(al.attrId)).toSet
  //associations
  val dttA2 = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain", ttA.id, 1, Some(0)),
    mutable.ArrayBuffer() ++ ttA.attributes.filter(al => Seq(2, 3).contains(al.attrId)),
    pkA2,
    versions.map(t => (t, Set(pkA2.head.valueAt(t)._2.attr.get))).toMap,
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  val dttA3 = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain", ttA.id, 1, Some(1)),
    mutable.ArrayBuffer() ++ ttA.attributes.filter(al => Seq(2, 4).contains(al.attrId)),
    pkA2,
    versions.map(t => (t, Set(pkA2.head.valueAt(t)._2.attr.get))).toMap,
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  val dttA1 = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain", ttA.id, 0, Some(0)),
    mutable.ArrayBuffer() ++ ttA.attributes.filter(al => Seq(0, 1).contains(al.attrId)),
    pkA1,
    versions.map(t => (t, Set(pkA1.head.valueAt(t)._2.attr.get))).toMap,
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  //B
  val pkB1 = ttB.attributes.filter(al => Seq(0).contains(al.attrId)).toSet
  val pkB2 = ttB.attributes.filter(al => Seq(2).contains(al.attrId)).toSet
  val dttB2 = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain", ttB.id, 1, Some(0)),
    mutable.ArrayBuffer() ++ ttB.attributes.filter(al => Seq(2, 3).contains(al.attrId)),
    pkB2,
    versions.map(t => (t, Set(pkB2.head.valueAt(t)._2.attr.get))).toMap,
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  val dttB1 = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain", ttB.id, 0, Some(0)),
    mutable.ArrayBuffer() ++ ttB.attributes.filter(al => Seq(0, 1).contains(al.attrId)),
    pkB1,
    versions.map(t => (t, Set(pkB1.head.valueAt(t)._2.attr.get))).toMap,
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  //C
  val pkC1 = ttC.attributes.filter(al => Seq(1).contains(al.attrId)).toSet
  val pkC2 = ttC.attributes.filter(al => Seq(3).contains(al.attrId)).toSet
  val dttC2 = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain", ttC.id, 1, Some(0)),
    mutable.ArrayBuffer() ++ ttC.attributes.filter(al => Seq(2, 3).contains(al.attrId)),
    pkC2,
    versions.map(t => (t, Set(pkC2.head.valueAt(t)._2.attr.get))).toMap,
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  val dttC1 = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain", ttC.id, 0, Some(0)),
    mutable.ArrayBuffer() ++ ttC.attributes.filter(al => Seq(0, 1).contains(al.attrId)),
    pkC1,
    versions.map(t => (t, Set(pkC1.head.valueAt(t)._2.attr.get))).toMap,
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  //D
  val pkD1 = ttD.attributes.filter(al => Seq(1).contains(al.attrId)).toSet
  val pkD2 = pkD1
  val pkD3 = ttD.attributes.filter(al => Seq(3).contains(al.attrId)).toSet
  val dttD3 = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain", ttD.id, 1, Some(0)),
    mutable.ArrayBuffer() ++ ttD.attributes.filter(al => Seq(2, 3).contains(al.attrId)),
    pkD3,
    versions.map(t => (t, Set(pkD3.head.valueAt(t)._2.attr.get))).toMap,
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  val dttD1 = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain", ttD.id, 0, Some(0)),
    mutable.ArrayBuffer() ++ ttD.attributes.filter(al => Seq(0, 1).contains(al.attrId)),
    pkD1,
    versions.map(t => (t, Set(pkD1.head.valueAt(t)._2.attr.get))).toMap,
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  val dttD2 = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain", ttD.id, 0, Some(1)),
    mutable.ArrayBuffer() ++ ttD.attributes.filter(al => Seq(4, 1).contains(al.attrId)),
    pkD2,
    versions.map(t => (t, Set(pkD1.head.valueAt(t)._2.attr.get))).toMap,
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  //splitPK
  val pkSplitPK = ttSplitPK.attributes.filter(al => Seq(0,2).contains(al.attrId)).toSet
  val dttSplitPK = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain", ttSplitPK.id, 0, Some(0)),
    mutable.ArrayBuffer() ++ ttSplitPK.attributes,
    pkSplitPK,
    Map(versions(0)-> Set(pkSplitPK.filter(_.attrId==0).head.valueAt(versions(0))._2.attr.get),
    versions(1) -> Set(pkSplitPK.filter(_.attrId==2).head.valueAt(versions(1))._2.attr.get)),
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  //normalPK
  val pkNormalPK = ttNormalPk.attributes.filter(al => Seq(0).contains(al.attrId)).toSet
  val dttNormalPK = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain", ttNormalPk.id, 0, Some(0)),
    mutable.ArrayBuffer() ++ ttNormalPk.attributes,
    pkNormalPK,
    versions.map(t => (t, Set(pkNormalPK.head.valueAt(t)._2.attr.get))).toMap,
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  //non-assocaitions
  val dttA1NonAssocation = getNonAssociation(dttA1)
  val dttB1NonAssocation = getNonAssociation(dttB1)
  val dttB2NonAssocation = getNonAssociation(dttB2)
  val dttC1NonAssocation = getNonAssociation(dttC1)
  val dttC2NonAssocation = getNonAssociation(dttC2)
  val dttA2NonAssocation = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain", ttA.id, 1, None),
    mutable.ArrayBuffer() ++ ttA.attributes.filter(al => Seq(2, 3, 4).contains(al.attrId)),
    pkA2,
    versions.map(t => (t, Set(pkA2.head.valueAt(t)._2.attr.get))).toMap,
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  val dttD3NonAssociation = getNonAssociation(dttD3)
  val dttD1And2NonAssocation = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain", ttD.id, 1, None),
    mutable.ArrayBuffer() ++ ttD.attributes.filter(al => Seq(0, 1, 4).contains(al.attrId)),
    pkD1,
    versions.map(t => (t, Set(pkD1.head.valueAt(t)._2.attr.get))).toMap,
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  val dttNormalPKNonAssocation = getNonAssociation(dttNormalPK)
  val dttSplitPKNonAssocation = getNonAssociation(dttSplitPK)
  val temporallyDecomposedTables = IndexedSeq(dttA1, dttA2, dttA3, dttB1, dttB2, dttC1, dttC2,dttD1,dttD2,dttD3,dttSplitPK,dttNormalPK)
  //write decomposed temporal tables:
  Seq(dttA1NonAssocation, dttA2NonAssocation, dttB1NonAssocation, dttB2NonAssocation, dttC1NonAssocation, dttC2NonAssocation,dttD1And2NonAssocation,dttD3NonAssociation,dttNormalPK,dttSplitPK).foreach(_.writeToStandardFile())
  //write decomposed associations:
  temporallyDecomposedTables.foreach(_.writeToStandardFile())
  //write table sketches of associations:
//  Seq((ttA,dttA1),(ttA,dttA2),(ttA,dttA3),(ttB,dttB1),(ttB,dttB2),(ttC,dttC1),(ttC,dttC2),(ttD,dttD1),(ttD,dttD2),(ttD,dttD3),(ttSplitPK,dttSplitPK),(ttNormalPk,dttNormalPK))
//    .foreach{case (tt,dtt) =>   tt.project(dtt).projection.writeTableSketch(dtt.primaryKey.map(_.attrId))
//    }
//

//
//  val nChangesInDecomposedTemporalTables = temporallyDecomposedTables
//    .map(dtt => SynthesizedTemporalDatabaseTable
//      .initFrom(dtt)
//      .countChanges(GLOBAL_CONFIG.CHANGE_COUNT_METHOD)).reduce(_ + _)
//  val topDownOptimizer = new TopDownOptimizer(temporallyDecomposedTables,nChangesInDecomposedTemporalTables,Set(),Map())
//  val db = topDownOptimizer.optimize()
//  db.finalSynthesizedTableIDs.foreach(id => {
    //    val synthTable = SynthesizedTemporalDatabaseTable.loadFromStandardFile(id)
    //    runAttributeMappingIntegrityCheck(synthTable)
    //    synthTable.rows.foreach(r => {
    //      val synthRow = r.asInstanceOf[SynthesizedTemporalRow]
    //      runRowIntegrityCheck(synthRow)
    //    })
    //  })

    def getNonAssociation(dttAssocaition: DecomposedTemporalTable) = {
      new DecomposedTemporalTable(DecomposedTemporalTableIdentifier(dttAssocaition.id.subdomain, dttAssocaition.id.viewID, dttAssocaition.id.bcnfID, None),
        dttAssocaition.containedAttrLineages, dttAssocaition.primaryKey, dttAssocaition.primaryKeyByVersion, mutable.HashSet[DecomposedTemporalTableIdentifier]())
    }


//
}
