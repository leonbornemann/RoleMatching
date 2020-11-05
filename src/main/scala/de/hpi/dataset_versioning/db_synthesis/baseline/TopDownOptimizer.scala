package de.hpi.dataset_versioning.db_synthesis.baseline

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based.{SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch, SynthesizedTemporalDatabase, SynthesizedTemporalDatabaseTable}
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.natural_key_based.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.surrogate_based.SurrogateBasedDecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.{DataBasedMatchCalculator, MatchCandidateGraph, TableUnionMatch}
import de.hpi.dataset_versioning.db_synthesis.database.GlobalSurrogateRegistry
import de.hpi.dataset_versioning.db_synthesis.database.query_tracking.ViewQueryTracker
import de.hpi.dataset_versioning.db_synthesis.database.table.{AssociationSchema, BCNFTableSchema}
import de.hpi.dataset_versioning.db_synthesis.sketches.table.SynthesizedTemporalDatabaseTableSketch

import scala.collection.mutable

class TopDownOptimizer(associations: IndexedSeq[AssociationSchema],
                       bcnfReferenceSchemata:collection.IndexedSeq[BCNFTableSchema],
                       nChangesInAssociations:Long,
                       extraNonDecomposedViewTableChanges:Map[String,Long],
                       tracker:Option[ViewQueryTracker] = None) extends StrictLogging{
  GlobalSurrogateRegistry.initSurrogateIDCounters(associations)
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
    mutable.HashSet() ++ associations
      .map(dtt => {
        val t = SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch.loadFromStandardOptimizationInputFile(dtt)
        read +=1
        if(read%100==0) {
          logger.debug(s"read $read/${associations.size} association sketches (${100*read/associations.size.toDouble}%)")
        }
      t
    })
  }

  val synthesizedDatabase = new SynthesizedTemporalDatabase(associations,bcnfReferenceSchemata,nChangesInAssociations,extraNonDecomposedViewTableChanges,tracker)
  private val matchCandidateGraph = new MatchCandidateGraph(allAssociationSketches,new DataBasedMatchCalculator())

  def executeMatch(bestMatch: TableUnionMatch[Int]) :(Option[SurrogateBasedSynthesizedTemporalDatabaseTableAssociation],SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch) = {
    //load the actual table
    val sketchA = bestMatch.firstMatchPartner.asInstanceOf[SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch]
    val sketchB = bestMatch.secondMatchPartner.asInstanceOf[SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch]
    val synthTableA = synthesizedDatabase.loadSynthesizedTable(sketchA)
    val synthTableB = synthesizedDatabase.loadSynthesizedTable(sketchB)
    val matchCalculator = new DataBasedMatchCalculator()
    val matchForSynth = matchCalculator.calculateMatch(synthTableA,synthTableB,true)
    if(matchForSynth.score>0){
      val bestMatchWithTupleMapping = matchCalculator.calculateMatch(sketchA,sketchB,true)
      val sketchOfUnion = sketchA.executeUnion(sketchB,bestMatchWithTupleMapping).asInstanceOf[SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch]
      val synthTableUnion = synthTableA.executeUnion(synthTableB,matchForSynth).asInstanceOf[SurrogateBasedSynthesizedTemporalDatabaseTableAssociation]
      (Some(synthTableUnion),sketchOfUnion)
    } else{
      (None,null)
    }
  }

  def optimize() = {
    var done = false
    while(!matchCandidateGraph.isEmpty && !done){
      logger.debug("Entering new Main loop iteration")
      if(matchCandidateGraph.getNextBestHeuristicMatch().score==0) {
        logger.debug("Terminating main loop as no more promising matches are available")
        done = true
      } else {
        val bestMatch = matchCandidateGraph.getNextBestHeuristicMatch()
        assert(bestMatch.isHeuristic)
        val (synthTable,synthTableSketch) = executeMatch(bestMatch)
        if(synthTable.isDefined){
          logger.debug(s"Unioning ${bestMatch.firstMatchPartner.informativeTableName} and ${bestMatch.secondMatchPartner.informativeTableName}")
          matchCandidateGraph.updateGraphAfterMatchExecution(bestMatch,synthTable.get,synthTableSketch)
          synthesizedDatabase.updateSynthesizedDatabase(synthTable.get,synthTableSketch,bestMatch)
          synthesizedDatabase.printState()
        } else{
          logger.debug("Heuristic match was erroneous - we remove this from the matches and continue")
          matchCandidateGraph.removeMatch(bestMatch)
        }
      }
    }
    //the final synthesized database is assembled:
    logger.debug("the final synthesized database is assembled")
    synthesizedDatabase.printState()
    synthesizedDatabase.writeToStandardFiles()
    synthesizedDatabase
  }


}
