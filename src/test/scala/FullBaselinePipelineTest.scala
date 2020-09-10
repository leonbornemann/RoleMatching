import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.ChangeExporter
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.{SynthesizedTemporalDatabaseTable, SynthesizedTemporalRow, TopDownOptimizer}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.{DecomposedTemporalTable, DecomposedTemporalTableIdentifier}
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

object FullBaselinePipelineTest extends App {
  IOService.socrataDir = "/home/leon/data/dataset_versioning/socrata/testDir/"
  val versions = IndexedSeq(LocalDate.parse("2019-11-01"), LocalDate.parse("2019-11-02"))
  val idA = "fullBaselineTest-A"
  val idB = "fullBaselineTest-B"
  val idC = "fullBaselineTest-C"
  val idD = "fullBaselineTest-D"
  val exporter = new ChangeExporter
  exporter.exportAllChangesFromVersions(idA, versions)
  exporter.exportAllChangesFromVersions(idB, versions)
  exporter.exportAllChangesFromVersions(idC, versions)
  exporter.exportAllChangesFromVersions(idD, versions)
  //how to proceed?
  val ttA = TemporalTable.load(idA)
  val ttB = TemporalTable.load(idB)
  val ttC = TemporalTable.load(idC)
  val ttD = TemporalTable.load(idD)
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
  val temporallyDecomposedTables = IndexedSeq(dttA1, dttA2, dttA3, dttB1, dttB2, dttC1, dttC2,dttD1,dttD2,dttD3)
  //write decomposed temporal tables:
  Seq(dttA1NonAssocation, dttA2NonAssocation, dttB1NonAssocation, dttB2NonAssocation, dttC1NonAssocation, dttC2NonAssocation,dttD1And2NonAssocation,dttD3NonAssociation).foreach(_.writeToStandardFile())
  //write decomposed associations:
  temporallyDecomposedTables.foreach(_.writeToStandardFile())
  //write table sketches of associations:
  ttA.project(dttA1).projection.writeTableSketch(dttA1.primaryKey.map(_.attrId))
  ttA.project(dttA2).projection.writeTableSketch(dttA2.primaryKey.map(_.attrId))
  ttA.project(dttA3).projection.writeTableSketch(dttA3.primaryKey.map(_.attrId))
  ttB.project(dttB1).projection.writeTableSketch(dttB1.primaryKey.map(_.attrId))
  ttB.project(dttB2).projection.writeTableSketch(dttB2.primaryKey.map(_.attrId))
  ttC.project(dttC1).projection.writeTableSketch(dttC1.primaryKey.map(_.attrId))
  ttC.project(dttC2).projection.writeTableSketch(dttC2.primaryKey.map(_.attrId))
  ttD.project(dttD1).projection.writeTableSketch(dttD1.primaryKey.map(_.attrId))
  ttD.project(dttD2).projection.writeTableSketch(dttD2.primaryKey.map(_.attrId))
  ttD.project(dttD3).projection.writeTableSketch(dttD3.primaryKey.map(_.attrId))
  temporallyDecomposedTables

  def getNonAssociation(dttAssocaition: DecomposedTemporalTable) = {
    new DecomposedTemporalTable(DecomposedTemporalTableIdentifier(dttAssocaition.id.subdomain, dttAssocaition.id.viewID, dttAssocaition.id.bcnfID, None),
      dttAssocaition.containedAttrLineages, dttAssocaition.primaryKey, dttAssocaition.primaryKeyByVersion, mutable.HashSet[DecomposedTemporalTableIdentifier]())
  }

  val nChangesInDecomposedTemporalTables = temporallyDecomposedTables.map(dtt => SynthesizedTemporalDatabaseTable.initFrom(dtt).numChanges.toLong).reduce(_ + _)
  val topDownOptimizer = new TopDownOptimizer(temporallyDecomposedTables,nChangesInDecomposedTemporalTables)
  val db = topDownOptimizer.optimize()

  def runRowIntegrityCheck(synthRow: SynthesizedTemporalRow) = {
    if(synthRow.fields.contains(ValueLineage(mutable.TreeMap(versions(0)->"00000")))){
      assert(synthRow.tupleIDTOViewTupleIDs.keySet == Set(idA,idB,idC,idD))
      assert(synthRow.tupleIDTOViewTupleIDs(idA) == Set(0,1))
    } else{
      assert(!synthRow.tupleIDTOViewTupleIDs.isEmpty && !synthRow.tupleIDToDTTTupleID.isEmpty)
    }
  }

  def runAttributeMappingIntegrityCheck(synthTable: SynthesizedTemporalDatabaseTable) = {
    if(synthTable.unionedTables == Set(dttA2.id,dttB2.id,dttC1.id,dttD1.id,dttD2.id)){
      val cityAttr = synthTable.nonKeyAttributeLineages.head
      assert(synthTable.nonKeyAttributeLineages.size==1)
      assert(synthTable.schemaTracking(cityAttr)(dttA2.id) == Set(3))
      assert(synthTable.schemaTracking(cityAttr)(dttB2.id) == Set(3))
      assert(synthTable.schemaTracking(cityAttr)(dttC1.id) == Set(0))
      assert(synthTable.schemaTracking(cityAttr)(dttD1.id) == Set(0))
      assert(synthTable.schemaTracking(cityAttr)(dttD2.id) == Set(4))
    }
  }

  db.finalSynthesizedTableIDs.foreach(id => {
    val synthTable = SynthesizedTemporalDatabaseTable.loadFromStandardFile(id)
    runAttributeMappingIntegrityCheck(synthTable)
    synthTable.rows.foreach(r => {
      val synthRow = r.asInstanceOf[SynthesizedTemporalRow]
      runRowIntegrityCheck(synthRow)
    })
  })
}
