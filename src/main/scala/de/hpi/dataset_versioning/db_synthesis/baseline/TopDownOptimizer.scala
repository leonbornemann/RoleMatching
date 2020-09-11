package de.hpi.dataset_versioning.db_synthesis.baseline

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.heuristics.{DataBasedMatchCalculator, MetadataBasedHeuristicMatchCalculator, TemporalDatabaseTableTrait}
import de.hpi.dataset_versioning.db_synthesis.sketches.SynthesizedTemporalDatabaseTableSketch

import scala.collection.mutable

class TopDownOptimizer(associations: IndexedSeq[DecomposedTemporalTable],nChangesInAssociations:Long,tracker:Option[ViewQueryTracker] = None) extends StrictLogging{
  println(s"initilized with:")
  associations.map(_.informativeTableName).sorted.foreach(println(_))
  assert(associations.forall(_.isAssociation))
  private val allAssociationSketches = mutable.HashSet() ++ associations.map(dtt => SynthesizedTemporalDatabaseTableSketch.initFrom(dtt))
//  Useful Debug statement but too expensive in full run (needs to load all tables)
  associations.foreach(as => {
    val table = SynthesizedTemporalDatabaseTable.initFrom(as)
    logger.debug(table.informativeTableName)
    table.printTable
    println("Heuristic:")
    val sketch = allAssociationSketches.filter(_.unionedTables.contains(as.id)).head
    logger.debug(sketch.informativeTableName)
    sketch.printTable
  })
  val synthesizedDatabase = new SynthesizedTemporalDatabase(associations,allAssociationSketches,nChangesInAssociations,tracker)
  private val matchCandidateGraph = new MatchCandidateGraph(allAssociationSketches,new DataBasedMatchCalculator())
  //for debugging:
  println()
  val a = synthesizedDatabase.loadSynthesizedTable(allAssociationSketches.filter(_.getID.contains("Normal")).head)
  val b = synthesizedDatabase.loadSynthesizedTable(allAssociationSketches.filter(_.getID.contains("Split")).head)
  val res = new DataBasedMatchCalculator().calculateMatch[Any](a,b)


  def executeMatch(bestMatch: TableUnionMatch[Int]) :(Option[SynthesizedTemporalDatabaseTable],SynthesizedTemporalDatabaseTableSketch) = {
    //load the actual table
    val sketchA = bestMatch.firstMatchPartner.asInstanceOf[SynthesizedTemporalDatabaseTableSketch]
    val sketchB = bestMatch.secondMatchPartner.asInstanceOf[SynthesizedTemporalDatabaseTableSketch]
    val synthTableA:SynthesizedTemporalDatabaseTable = synthesizedDatabase.loadSynthesizedTable(sketchA)
    val synthTableB:SynthesizedTemporalDatabaseTable = synthesizedDatabase.loadSynthesizedTable(sketchB)
    val matchCalculator = new DataBasedMatchCalculator()
    val matchForSynth = matchCalculator.calculateMatch(synthTableA,synthTableB,true)
    if(matchForSynth.score>0){
      val bestMatchWithTupleMapping = matchCalculator.calculateMatch(sketchA,sketchB,true)
      val sketchOfUnion = sketchA.executeUnion(sketchB,bestMatchWithTupleMapping).asInstanceOf[SynthesizedTemporalDatabaseTableSketch]
      val synthTableUnion = synthTableA.executeUnion(synthTableB,matchForSynth).asInstanceOf[SynthesizedTemporalDatabaseTable]
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
