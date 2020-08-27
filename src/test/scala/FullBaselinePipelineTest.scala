import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.ChangeExporter
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.TopDownOptimizer
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.{DecomposedTemporalTable, DecomposedTemporalTableIdentifier}
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

object FullBaselinePipelineTest extends App {
  IOService.socrataDir = "/home/leon/data/dataset_versioning/socrata/testDir/"
  val exporter = new ChangeExporter
  val idA = "fullBaselineTest-A"
  val idB = "fullBaselineTest-B"
  val idC = "fullBaselineTest-C"
  val versions = IndexedSeq(LocalDate.parse("2019-11-01"),LocalDate.parse("2019-11-02"))
  exporter.exportAllChangesFromVersions(idA,versions)
  exporter.exportAllChangesFromVersions(idB,versions)
  exporter.exportAllChangesFromVersions(idC,versions)
  //how to proceed?
  val ttA = TemporalTable.load(idA)
  val ttB = TemporalTable.load(idB)
  val ttC = TemporalTable.load(idC)
  //A
  private val pkA1 = ttA.attributes.filter(al => Seq(0).contains(al.attrId)).toSet
  private val pkA2 = ttA.attributes.filter(al => Seq(2).contains(al.attrId)).toSet
  val dttA2 = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain",ttA.id,1,Some(1)),
    mutable.ArrayBuffer() ++ ttA.attributes.filter(al => Seq(2,3).contains(al.attrId)),
    pkA2,
    versions.map(t => (t,Set(pkA2.head.valueAt(t)._2.attr.get))).toMap,
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  val dttA1 = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain",ttA.id,0,Some(0)),
    mutable.ArrayBuffer() ++ ttA.attributes.filter(al => Seq(0,1).contains(al.attrId)),
    pkA1,
    versions.map(t => (t,Set(pkA1.head.valueAt(t)._2.attr.get))).toMap,
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  //B
  private val pkB1 = ttB.attributes.filter(al => Seq(0).contains(al.attrId)).toSet
  private val pkB2 = ttB.attributes.filter(al => Seq(2).contains(al.attrId)).toSet
  val dttB2 = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain",ttB.id,1,Some(1)),
    mutable.ArrayBuffer() ++ ttB.attributes.filter(al => Seq(2,3).contains(al.attrId)),
    pkB2,
    versions.map(t => (t,Set(pkB2.head.valueAt(t)._2.attr.get))).toMap,
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  val dttB1 = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain",ttB.id,0,Some(0)),
    mutable.ArrayBuffer() ++ ttB.attributes.filter(al => Seq(0,1).contains(al.attrId)),
    pkB1,
    versions.map(t => (t,Set(pkB1.head.valueAt(t)._2.attr.get))).toMap,
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  //C
  private val pkC1 = ttC.attributes.filter(al => Seq(1).contains(al.attrId)).toSet
  private val pkC2 = ttC.attributes.filter(al => Seq(3).contains(al.attrId)).toSet
  val dttC2 = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain",ttC.id,1,Some(1)),
    mutable.ArrayBuffer() ++ ttC.attributes.filter(al => Seq(2,3).contains(al.attrId)),
    pkC2,
    versions.map(t => (t,Set(pkC2.head.valueAt(t)._2.attr.get))).toMap,
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  val dttC1 = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain",ttC.id,0,Some(0)),
    mutable.ArrayBuffer() ++ ttC.attributes.filter(al => Seq(0,1).contains(al.attrId)),
    pkC1,
    versions.map(t => (t,Set(pkC1.head.valueAt(t)._2.attr.get))).toMap,
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  val temporallyDecomposedTables = IndexedSeq(dttA1,dttA2,dttB1,dttB2,dttC1,dttC2)
  //write table sketches
  ttA.project(dttA1).projection.writeTableSketch()
  ttA.project(dttA2).projection.writeTableSketch()
  ttB.project(dttB1).projection.writeTableSketch()
  ttB.project(dttB2).projection.writeTableSketch()
  ttC.project(dttC1).projection.writeTableSketch()
  ttC.project(dttC2).projection.writeTableSketch()
  val topDownOptimizer = new TopDownOptimizer(temporallyDecomposedTables)
  topDownOptimizer.optimize()
}
