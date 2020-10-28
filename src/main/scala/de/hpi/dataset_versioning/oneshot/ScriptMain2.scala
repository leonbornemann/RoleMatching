package de.hpi.dataset_versioning.oneshot

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.{ChangeCube, ChangeExporter}
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.surrogate_based.SurrogateBasedDecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.sketches.table.{DecomposedTemporalTableSketch, SynthesizedTemporalDatabaseTableSketch}
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}
import de.hpi.dataset_versioning.oneshot.ScriptMain.args

import scala.sys.process._
import scala.language.postfixOps

object ScriptMain2 extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
  val subdomainIds = subDomainInfo(subdomain)
    .map(_.id)
    .toIndexedSeq
  private val date = LocalDate.parse("2019-11-01")
  IOService.cacheMetadata(date)
  subdomainIds.foreach(id => {
    val md = IOService.cachedMetadata(date)(id)
    if(md.resource.name.contains("School")){
      println(md.resource.name)
    }
  })

}
