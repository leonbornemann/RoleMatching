package de.hpi.dataset_versioning.db_synthesis.baseline

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based.SynthesizedTemporalDatabase
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.{SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch}
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.{DataBasedMatchCalculator, MatchCandidateGraph, TableUnionMatch}
import de.hpi.dataset_versioning.db_synthesis.database.GlobalSurrogateRegistry
import de.hpi.dataset_versioning.db_synthesis.database.table.{AssociationSchema, BCNFTableSchema}

import scala.collection.mutable

class TopDownOptimizer(associations: IndexedSeq[AssociationSchema],
                       bcnfReferenceSchemata:collection.IndexedSeq[BCNFTableSchema],
                       nChangesInAssociations:Long,
                       extraNonDecomposedViewTableChanges:Map[String,Long]) extends StrictLogging{
  GlobalSurrogateRegistry.initSurrogateIDCounters(associations)

//  logger.debug("Executing Histogram")
//  val histogramFile = new PrintWriter("/home/leon/data/dataset_versioning/socrata/value_histogram.csv")
//  associations.foreach(a => {
//    val table = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(a.id)
//    IOService.STANDARD_TIME_RANGE.foreach(ts => {
//      val values = table.rows.map(r => r.valueLineage.valueAt(ts))
//        .groupBy(identity)
//        .map(t => (t._1,t._2.size))
//        .filter(_._2 > 100)
//      values.foreach{case (v,count) => histogramFile.println(s"$ts,${if(v!=null) v.toString.replaceAll(",",";") else "null"},$count")}
//    })
//    logger.debug(s"finished ${a}")
//  })
//  histogramFile.close()
//  logger.debug("Finished executing histogram")

//  println(s"initilized with:")
//  associations.map(_.informativeTableName).sorted.foreach(println(_))
  private val allAssociationSketches = loadAssociationSketches()

  //  Useful Debug statement but too expensive in full run (needs to load all tables)
//  associations.foreach(as => {
//    val table = SynthesizedTemporalDatabaseTable.initFrom(as)
//    logger.debug(table.informativeTableName)
//    table.printTable
//    println("Heuristic:")
//    val sketch = allAssociationSketches.filter(_.unionedTables.contains(as.id)).head
//    logger.debug(sketch.informativeTableName)
//    sketch.printTable
//  })

  private def loadAssociationSketches() = {
    var read = 0
    var hasChanges = 0
    mutable.HashSet() ++ associations
      .map(dtt => {
        val t = SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch.loadFromStandardOptimizationInputFile(dtt)
        if(GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.countChanges(t)>0){
          hasChanges +=1
        }
        read +=1
        if(read%100==0) {
          logger.debug(s"read $read/${associations.size} association sketches (${100*read/associations.size.toDouble}%) of which ${hasChanges} have changes (${100*hasChanges /read.toDouble}%)")
        }
      t
    })
  }

  val synthesizedDatabase = new SynthesizedTemporalDatabase(associations,bcnfReferenceSchemata,nChangesInAssociations,extraNonDecomposedViewTableChanges)
  private val matchCandidateGraph = new MatchCandidateGraph(allAssociationSketches,new DataBasedMatchCalculator())

  def executeMatch(bestMatch: TableUnionMatch[Int]):Option[ExecutedTableUnion] = {
    //load the actual table
    val sketchA = bestMatch.firstMatchPartner.asInstanceOf[SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch]
    val sketchB = bestMatch.secondMatchPartner.asInstanceOf[SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch]
    val synthTableA = synthesizedDatabase.loadSynthesizedTable(sketchA)
    val synthTableB = synthesizedDatabase.loadSynthesizedTable(sketchB)
    val matchCalculator = new DataBasedMatchCalculator()
    val matchForSynth = matchCalculator.calculateMatch(synthTableA,synthTableB,true)
    if(matchForSynth.score>0){
      val matchForSketchWithTupleMapping = matchCalculator.calculateMatch(sketchA,sketchB,true)
      val (sketchOfUnion,sketchTupleMapping) = matchForSketchWithTupleMapping.buildUnionedTable  //sketchA.executeUnion(sketchB,matchForSketchWithTupleMapping).asInstanceOf[SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch]
      val (synthTableUnion,synthTupleMapping) = matchForSynth.buildUnionedTable //  synthTableA.executeUnion(synthTableB,matchForSynth).asInstanceOf[SurrogateBasedSynthesizedTemporalDatabaseTableAssociation]
      val executedUnion = ExecutedTableUnion(matchForSketchWithTupleMapping,
        sketchOfUnion.asInstanceOf[SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch],
        sketchTupleMapping,
        matchForSynth,
        synthTableUnion.asInstanceOf[SurrogateBasedSynthesizedTemporalDatabaseTableAssociation],
        synthTupleMapping
      )
      Some(executedUnion)
    } else{
      None
    }
  }

  def optimize() = {
    var done = false
    while(!matchCandidateGraph.isEmpty && !done){
      logger.debug("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
      logger.debug("Entering new Main loop iteration")
      if(matchCandidateGraph.getNextBestHeuristicMatch().score==0) {
        logger.debug("Terminating main loop as no more promising matches are available")
        done = true
      } else {
        val bestMatch = matchCandidateGraph.getNextBestHeuristicMatch()
        assert(bestMatch.isHeuristic)
        val matchResult = executeMatch(bestMatch)
        if(matchResult.isDefined){
          val executedUnion = matchResult.get
          logger.debug(s"Unioning ${bestMatch.firstMatchPartner} and ${bestMatch.secondMatchPartner}")
          matchCandidateGraph.updateGraphAfterMatchExecution(bestMatch,executedUnion.unionedTableSketch)
          synthesizedDatabase.updateSynthesizedDatabase(executedUnion)
          synthesizedDatabase.printState()
        } else{
          logger.debug("Heuristic match was erroneous - we remove this from the matches and continue")
          matchCandidateGraph.removeMatch(bestMatch)
        }
      }
      logger.debug("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
    }
    //the final synthesized database is assembled:
    logger.debug("the final synthesized database is assembled")
    synthesizedDatabase.printState()
    synthesizedDatabase.generateQueries()
    synthesizedDatabase.writeToStandardFiles()
    synthesizedDatabase
  }


}
