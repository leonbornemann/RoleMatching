package de.hpi.role_matching.cbrm.evidence_based_weighting

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.compatibility_graph.representation.slim.MemoryEfficientCompatiblityGraphWithoutEdgeWeight
import de.hpi.role_matching.cbrm.data.Roleset

import scala.collection.parallel.CollectionConverters._
import java.io.File
import java.time.LocalDate

object TuningDataExportMultipleWikipediaDatasetsMain extends App with StrictLogging{
  // ls /san2/data/change-exploration/roleMerging/finalExperiments/newWikipediaRolesets/NO_DECAY_7_2011-05-07/
  //education.json  football.json  military.json  politics.json  tv_and_film.json


  println(s"Called with ${args.toIndexedSeq}")
  val datasource = args(0)
  GLOBAL_CONFIG.setSettingsForDataSource(datasource)
  val simpleGraphRootDir = new File(args(1))
  val rolesetRootDir = args(2)
  simpleGraphRootDir.listFiles().par
    .foreach{configDir =>
      processConfigDir(configDir)
    }

  private def processConfigDir(configDir: File) = {
    logger.debug(s"Starting Config ${configDir.getName}")
    val curConfig = configDir.getName
    val curTrainTimeEnd = LocalDate.parse(curConfig.split("_").last)
    configDir.listFiles().par.foreach { dsDir =>
      processDatasetDir(configDir, curTrainTimeEnd, dsDir)
    }
    logger.debug(s"Finished Config ${configDir.getName}")
  }

  private def processDatasetDir(configDir: File, curTrainTimeEnd: LocalDate, dsDir: File) = {
    val logPrefix = s"${dsDir.getName}_${configDir.getName}  --  "
    logger.debug(s"Starting Config ${configDir.getName} - Dataset ${dsDir.getName}")
    val statOutputFile = new File(dsDir.getAbsolutePath + "/tuningStats.csv")
    val graphOutputFile = new File(dsDir.getAbsolutePath + "/memoryEfficientGraphForOptimization.json")
    ///san2/data/change-exploration/roleMerging/finalExperiments/newWikipediaGraphs/NO_DECAY_7_2011-05-07/tv_and_film/edges/
    val edgeIDGraphDir = new File(dsDir.getAbsolutePath + "/edges/")
    val roleset = Roleset.fromJsonFile(s"$rolesetRootDir/${configDir.getName}/${dsDir.getName}.json")
    if(edgeIDGraphDir.exists() && !edgeIDGraphDir.listFiles().isEmpty){
      val simpleEdgeIterator = SimpleCompatbilityGraphEdge.iterableFromEdgeIDObjectPerLineDir(edgeIDGraphDir, roleset)
      val graph = MemoryEfficientCompatiblityGraphWithoutEdgeWeight.fromGeneralEdgeIterator(simpleEdgeIterator, GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, curTrainTimeEnd, Seq())
      val isfMaps = graph.getISFMapsAtEndTimes(Array(curTrainTimeEnd))
      val counter = new EvidenceBasedWeightingEventCounter(graph, isfMaps, GLOBAL_CONFIG.granularityInDays, statOutputFile, graphOutputFile,logPrefix)
      counter.aggregateEventCounts(GLOBAL_CONFIG.granularityInDays, 1000000) //we do some sampling so that the tuning experiments (python jupyter notebook) can be conveniently executed on a local machine

    } else {
      logger.debug(s"Skipping Config  ${configDir.getName} - Dataset ${dsDir.getName} - no edge id files available")
    }
    logger.debug(s"Terminating Config ${configDir.getName} - Dataset ${dsDir.getName}")
  }
}
