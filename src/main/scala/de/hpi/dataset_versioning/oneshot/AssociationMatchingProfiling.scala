package de.hpi.dataset_versioning.oneshot

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.{SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.DataBasedMatchCalculator
import de.hpi.dataset_versioning.io.IOService

object AssociationMatchingProfiling extends App with StrictLogging{

  IOService.socrataDir = args(0)
  val subdomain = "org.cityofchicago"
  val ids = "a9u4-3dwb.0_0,cygx-ui4j.0_9,a9u4-3dwb.0_0,x2n5-8w5q.0_5,a9u4-3dwb.0_0,cygx-ui4j.4_2,m6dm-c72p.1_8,ir7v-8mc8.0_7,i9rk-duva.0_3,cygx-ui4j.1_57".split(",")
  val pairs = (0 until ids.size/2).map(i => (ids(i*2),ids(i*2+1)))
    .map(t => (DecomposedTemporalTableIdentifier.fromShortString(subdomain,t._1),DecomposedTemporalTableIdentifier.fromShortString(subdomain,t._2)))
  println(pairs)
  //a9u4-3dwb.0_0,cygx-ui4j.0_9,1030,1178,109.914515068
  //a9u4-3dwb.0_0,x2n5-8w5q.0_5,1030,1205,71.69991142
  //a9u4-3dwb.0_0,cygx-ui4j.4_2,1030,1178,67.462155002
  //m6dm-c72p.1_8,ir7v-8mc8.0_7,255,971,71.51682753
  //i9rk-duva.0_3,cygx-ui4j.1_57,1132,1124,67.879360563
  //val matchPairs = Seq((DecomposedTemporalTableIdentifier.fromShortString(subdomain,"a9u4-3dwb.0_0"),
  val matcher = new DataBasedMatchCalculator()
  logger.debug("beginning loop")
  pairs.foreach{case (a,b) => {
    println(s"beginning ${(a,b)}")
    val a1 = SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch.loadFromStandardOptimizationInputFile(a)
    val a2 = SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch.loadFromStandardOptimizationInputFile(b)
    val res = matcher.calculateMatch(a1,a2,false)
    println(res)
  }}
  logger.debug("finishing loop")
}
