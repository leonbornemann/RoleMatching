package de.hpi.dataset_versioning.oneshot

import de.hpi.dataset_versioning.data.change.{ChangeCube, ChangeExporter}
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.{DecomposedTemporalTable, SurrogateBasedDecomposedTemporalTable}
import de.hpi.dataset_versioning.db_synthesis.sketches.table.{DecomposedTemporalTableSketch, SynthesizedTemporalDatabaseTableSketch}
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

import scala.sys.process._
import scala.language.postfixOps

object ScriptMain2 extends App {

}
