package de.hpi.role_matching.cbrm.evidence_based_weighting

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.data.{ChangePoint, CommonPointOfInterestIterator, RoleLineage, Util, ValueTransition}
import de.hpi.role_matching.cbrm.data.json_serialization.{JsonReadable, JsonWritable}
import de.hpi.role_matching.cbrm.evidence_based_weighting.EventOccurrenceStatistics.{NEUTRAL, STRONGNEGATIVE, STRONGPOSTIVE, WEAKNEGATIVE, WEAKPOSTIVE}
import de.hpi.role_matching.cbrm.evidence_based_weighting.isf.ISFMapStorage

import java.time.LocalDate

case class EventOccurrenceStatistics(val trainTimeEnd:LocalDate,
                                     var summedScores:Option[collection.mutable.IndexedSeq[Float]]=Some(collection.mutable.ArrayBuffer[Float](0.0f,0.0f,0.0f,0.0f,0.0f)),
                                     var strongPositive:Int=0,
                                     var weakPositive:Int=0,
                                     var neutral:Int=0,
                                     var weakNegative:Int=0,
                                     var strongNegative:Int=0) extends JsonWritable[EventOccurrenceStatistics]{
  def totalCount = strongPositive+weakPositive+neutral+weakNegative+strongNegative

  def addScore(kind:String,count:Int,scoreSum:Float) = {
    kind match {
      case STRONGPOSTIVE  => strongPositive+=count; if(summedScores.isDefined) summedScores.get(0) = summedScores.get(0) + scoreSum
      case WEAKPOSTIVE  => weakPositive+=count; if(summedScores.isDefined) summedScores.get(1) = summedScores.get(1) + scoreSum
      case NEUTRAL  => neutral+=count; if(summedScores.isDefined) summedScores.get(2) = summedScores.get(2) + scoreSum
      case WEAKNEGATIVE  => weakNegative+=count; if(summedScores.isDefined) summedScores.get(3) = summedScores.get(3) + scoreSum
      case STRONGNEGATIVE  => strongNegative+=count; if(summedScores.isDefined) summedScores.get(4) = summedScores.get(4) + scoreSum
      case _  => assert(false)
    }
  }

  if(summedScores.isDefined)
    assert(summedScores.get.size==5) //positions of the score sums are the same as in the constructors

  def addAll(other: EventOccurrenceStatistics) = {
    strongPositive +=other.strongPositive
    weakPositive +=other.weakPositive
    neutral +=other.neutral
    weakNegative +=other.weakNegative
    strongNegative +=other.strongNegative
    if(summedScores.isDefined && other.summedScores.isDefined){
      for(i <- 0 until summedScores.get.size){
        summedScores.get(i)=summedScores.get(i)+other.summedScores.get(i)
      }
    }
  }

  def toDittoString = {
    Util.toDittoSaveString(s"COL SPC VAL $strongPositive COL WPC VAL $weakPositive COL NC VAL $neutral COL WNC VAL $weakNegative COL SNC VAL $strongNegative")
  }
}
object EventOccurrenceStatistics extends JsonReadable[EventOccurrenceStatistics]{

  val STRONGPOSTIVE = "strongPositive"
  val WEAKPOSTIVE = "weakPositive"
  val NEUTRAL = "neutral"
  val WEAKNEGATIVE = "weakNegative"
  val STRONGNEGATIVE = "strongNegative"

  ///Extracts full event counts for this edge (not cutoff at any train time end)
  def extractForEdge(id1:String,
                     id2:String,
                     rl1:RoleLineage,
                     rl2:RoleLineage,
                     trainTimeEnd:LocalDate,
                     transitionSets:Map[String, Set[ValueTransition]],
                     tfIDFMap:Map[ValueTransition,Int]) :EventOccurrenceStatistics = {
    val commonPointOfInterestIterator = new CommonPointOfInterestIterator(rl1, rl2)
    val statistic = new EventOccurrenceStatistics(trainTimeEnd)
    commonPointOfInterestIterator
      .foreach(cp => {
        statistic.addAll(getEventCounts(id1,id2,cp,trainTimeEnd,transitionSets,tfIDFMap))
      })
    statistic
  }

  def getEventCounts(vertexIdFirst:String,
                     vertexIdSecond:String,
                     cp: ChangePoint,
                     trainTimeEnd:LocalDate,
                     transitionSets:Map[String, Set[ValueTransition]],
                     tfIDFMap:Map[ValueTransition,Int]) = {
    assert(!cp.prevPointInTime.isAfter(trainTimeEnd))
    val totalCounts = new EventOccurrenceStatistics(trainTimeEnd)
    val countPrev = EvidenceBasedWeightingScoreComputer.getCountPrev(cp, GLOBAL_CONFIG.granularityInDays, Some(trainTimeEnd)).toInt
    if (countPrev > 0) {
      val countPrevTransiton = EvidenceBasedWeightingScoreComputer.getCountForSameValueTransition(cp.prevValueA, cp.prevValueB, countPrev, RoleLineage.isWildcard,
        transitionSets(vertexIdFirst), transitionSets(vertexIdSecond), GLOBAL_CONFIG.nonInformativeValues, false, Some(tfIDFMap))
      if (countPrevTransiton.isDefined)
        totalCounts.addAll(countPrevTransiton.get)
    }
    if (!cp.pointInTime.isAfter(trainTimeEnd)) {
      val countCurrent = EvidenceBasedWeightingScoreComputer.getCountForTransition(cp, RoleLineage.isWildcard,
        transitionSets(vertexIdFirst), transitionSets(vertexIdSecond), GLOBAL_CONFIG.nonInformativeValues, false, Some(tfIDFMap))
      //assert(countCurrent.isDefined)
      if (countCurrent.isDefined) {
        totalCounts.addAll(countCurrent.get)
      }
      //if this is the last one we have more same value transitions until the end of trainTimeEnd
      if (cp.isLast && countCurrent.isDefined && cp.pointInTime.isBefore(trainTimeEnd)) {
        val countAfterInDays = trainTimeEnd.toEpochDay - cp.pointInTime.toEpochDay - GLOBAL_CONFIG.granularityInDays
        if (!(countAfterInDays % GLOBAL_CONFIG.granularityInDays == 0))
          println()
        assert(countAfterInDays % GLOBAL_CONFIG.granularityInDays == 0)
        val countAfter = countAfterInDays / GLOBAL_CONFIG.granularityInDays
        val result = EvidenceBasedWeightingScoreComputer.getCountForSameValueTransition(cp.curValueA, cp.curValueB, countAfter.toInt, RoleLineage.isWildcard,
          transitionSets(vertexIdFirst), transitionSets(vertexIdSecond), GLOBAL_CONFIG.nonInformativeValues, false, Some(tfIDFMap))
        if (result.isDefined)
          totalCounts.addAll(result.get)
      }
    }
    totalCounts
  }


}
