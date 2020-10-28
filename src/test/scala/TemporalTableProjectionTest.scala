import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.ChangeExporter
import de.hpi.dataset_versioning.data.change.temporal_tables.{AttributeLineage, ProjectedTemporalRow, TemporalTable}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

object TemporalTableProjectionTest extends App {
  ???
//  IOService.socrataDir = "/home/leon/data/dataset_versioning/socrata/testDir/"
//  val exporter = new ChangeExporter
//  val id = "temporalTableProjectionTest"
//  val versions = IndexedSeq(LocalDate.parse("2019-11-01"),LocalDate.parse("2019-11-02"))
//  exporter.exportAllChangesFromVersions(id,versions)
//  val tt = TemporalTable.load(id)
//  private val attributes: collection.IndexedSeq[AttributeLineage] = tt.attributes
//  private val keyAttrInProjection = attributes.filter(_.attrId == 5).head
//  val projected = tt.project(DecomposedTemporalTable(DecomposedTemporalTableIdentifier("asd",tt.id,0,None),
//    mutable.ArrayBuffer() ++ attributes.filter(al => al.attrId==6 || al.attrId == 5),
//    Set(keyAttrInProjection),
//    Map(LocalDate.parse("2019-11-01") -> Set(keyAttrInProjection.valueAt(LocalDate.parse("2019-11-01"))._2.attr.get),
//      LocalDate.parse("2019-11-02") -> Set(keyAttrInProjection.valueAt(LocalDate.parse("2019-11-02"))._2.attr.get)),
//    mutable.HashSet[DecomposedTemporalTableIdentifier]())
//  )
//  assert(tt.rows.size > projected.projection.rows.size )
//  println(tt.rows.size)
//  println(projected.projection.rows.size)
//  assert(projected.projection.rows.exists(r => r.asInstanceOf[ProjectedTemporalRow].mappedEntityIds == Set(120,91)))
}
