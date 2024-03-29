package de.hpi.role_matching.data

import de.hpi.role_matching.blocking.rm.RoleDomain
import de.hpi.role_matching.data.RemainsValidVariant.RemainsValidVariant
import de.hpi.role_matching.data.RoleLineage.WILDCARD_VALUES
import de.hpi.role_matching.data.RoleLineageWithID.{digitRegex, printTabularEventLineageString}
import de.hpi.util.GLOBAL_CONFIG

import java.time.LocalDate
import java.time.temporal.ChronoUnit
import scala.collection.mutable
import scala.util.Random

@SerialVersionUID(3L)
case class RoleLineage(lineage:mutable.TreeMap[LocalDate,Any] = mutable.TreeMap[LocalDate,Any]()) extends Serializable{

  def removeSubsequentDuplicates() = {
    val withIndex = lineage
      .toIndexedSeq
      .zipWithIndex
    val toFilterOut = withIndex
      .filter{case ((ld,v),i) => i!=0 && v == withIndex(i-1)._1._2}
      .map(_._1._1)
    toFilterOut
      .foreach(ld => lineage.remove(ld))
  }


  def dataDensity(trainTimeEnd: LocalDate) = {
    nonWildcardDuration(trainTimeEnd.plusDays(1)) / ChronoUnit.DAYS.between(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, trainTimeEnd.plusDays(1)).toDouble
  }

  def valueToNonWildcardTimestamps(timePeriodEnd:LocalDate) = {
    val withIndex = lineage
      .toIndexedSeq
      .filter(t => t._1.isBefore(timePeriodEnd))
      .zipWithIndex
    val res = withIndex
      .withFilter(t => !isWildcard(t._1._2))
      .map{case ((ld,v),i) => {
        val end = if(i==withIndex.size-1) timePeriodEnd else withIndex(i+1)._1._1
        val dateRange = (ld.toEpochDay until end.toEpochDay by GLOBAL_CONFIG.granularityInDays)
          .map(l => LocalDate.ofEpochDay(l))
        (v,dateRange)
      }}
    res

  }

  def addSyntheticallyMissingData(targetShareOfAbsentValues: Double,
                                  timePeriodEnd:LocalDate,
                                  random:Random):RoleLineage = {
    val currentDensity = dataDensity(timePeriodEnd)
    val curWildcardShare = 1.0 - currentDensity
    if(curWildcardShare>= targetShareOfAbsentValues){
      this
    } else {
      val totalTimestamps = ChronoUnit.DAYS.between(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, timePeriodEnd) / GLOBAL_CONFIG.granularityInDays
      val missingNumberOfTimestamps = ((targetShareOfAbsentValues - curWildcardShare)*totalTimestamps).toInt
      val nonWCTS = valueToNonWildcardTimestamps(timePeriodEnd)
      //randomly choose missingNumberOfTimestamps
      val setToWildcard = random.shuffle(nonWCTS
        .flatMap(t => t._2.tail)) //only take tail so we never replace the first insert with missing data, this does not really impact the result, but gets rid of unwanted side-effects on the value distribution by replacing things with missing values
        .take(missingNumberOfTimestamps)
        .sorted
      val newLineage = collection.mutable.TreeMap[LocalDate,Any]() ++ lineage
      for (elem <- setToWildcard) {
        newLineage.put(elem,ReservedChangeValues.DECAYED)
        if(!lineage.contains(elem.plusDays(GLOBAL_CONFIG.granularityInDays))) {
          //change it back at the next timestamp:
          newLineage.put(elem.plusDays(GLOBAL_CONFIG.granularityInDays),lineage.maxBefore(elem).get._2)
        }
      }
      //eliminate subsequentDuplicates
      val newLineageWithIndex = newLineage
        .toIndexedSeq
        .zipWithIndex
      val newLineageFiltered = collection.mutable.TreeMap[LocalDate,Any]() ++ newLineageWithIndex
        .filter{case ((ld,v),i) => i==0 || v != newLineageWithIndex(i-1)._1._2}
        .map(_._1)
      val newRL = RoleLineage(newLineageFiltered)
      val newDensity = newRL.dataDensity(timePeriodEnd)
      val newTargetShareOfAbsentValues = 1.0-newDensity
//      if(!(newTargetShareOfAbsentValues <= targetShareOfAbsentValues+0.01 && newTargetShareOfAbsentValues >= targetShareOfAbsentValues-0.01))
//        println()
      assert(lineage.size>20 || newTargetShareOfAbsentValues <= targetShareOfAbsentValues+0.01 && newTargetShareOfAbsentValues >= targetShareOfAbsentValues-0.01)
      assert(newRL.nonWildCardValues.toSet == nonWildCardValues.toSet)
      newRL
    }

  }


  def presenceTimeDoesNotExactlyMatch(rl2ProjectedNoDecay: RoleLineage, trainTimeEnd: LocalDate): Boolean = {
    val me = toExactValueSequence(trainTimeEnd)
    val other = rl2ProjectedNoDecay.toExactValueSequence(trainTimeEnd)
    assert(me.size == other.size)
    me.zip(other)
      .exists { case (v, w) =>
        (isWildcard(v), isWildcard(w)) match {
          case (true, false) => true
          case (false, true) => true
          case _ => false
        }
      }
  }



  def exactDistinctMatchWithoutWildcardCount(rl2Projected: RoleLineage, until: LocalDate) = {
    val meWithoutDecay = RoleLineage(lineage.filter(_._2!=ReservedChangeValues.DECAYED))
    val otherWithoutDecay = RoleLineage(rl2Projected.lineage.filter(_._2!=ReservedChangeValues.DECAYED))
    val count = new CommonPointOfInterestIterator(meWithoutDecay,otherWithoutDecay)
      .filter(cp => cp.curValueA==cp.curValueB && !isWildcard(cp.curValueA) && !isWildcard(cp.curValueB))
      .map(cp => cp.curValueA)
      .toSet
      .size
    count
  }


  def toNewTimeScale(d: Int): RoleLineage = {
    val startOld = GLOBAL_CONFIG.STANDARD_TIME_FRAME_START
    val newLineage = lineage
      .map{case (ld,v) => (GLOBAL_CONFIG.STANDARD_TIME_FRAME_START.plusDays((ChronoUnit.DAYS.between(startOld,ld)*d).toInt),v)}
    RoleLineage(newLineage)
  }


  def applyDecay(decayThreshold: Double,trainTimeEnd:LocalDate) = {
    if(decayThreshold<1.0){
      val withIndex = lineage.toIndexedSeq.zipWithIndex
      val durationsInTrainTime = withIndex.tail
        .withFilter{ case ((ld, _), i) => !ld.isAfter(trainTimeEnd)}
        .map { case ((ld, _), i) => ChronoUnit.DAYS.between(withIndex(i - 1)._1._1, ld) }
        .sorted
      val indexOfCutoff = math.ceil(decayThreshold * durationsInTrainTime.size).toInt -1
      if(!durationsInTrainTime.isEmpty){
        val decayTimeInDays = if(indexOfCutoff<0) GLOBAL_CONFIG.granularityInDays else durationsInTrainTime(indexOfCutoff)
        assert((decayTimeInDays % GLOBAL_CONFIG.granularityInDays) == 0)
        val lineage = withIndex
          .flatMap { case ((ld, v), i) =>
            val endDate = if (i == withIndex.size-1) GLOBAL_CONFIG.STANDARD_TIME_FRAME_END else withIndex(i + 1)._1._1
            val duration = ChronoUnit.DAYS.between(ld, endDate)
            if (duration > decayTimeInDays && !RoleLineage.isWildcard(v))
              Seq((ld, v), (ld.plusDays(decayTimeInDays), ReservedChangeValues.DECAYED))
            else
              Seq((ld, v))
          }
        RoleLineage(collection.mutable.TreeMap[LocalDate,Any]() ++ lineage)
      } else {
        RoleLineage(collection.mutable.TreeMap[LocalDate,Any]() ++ withIndex.map(_._1))
      }
    } else {
      RoleLineage(collection.mutable.TreeMap[LocalDate,Any]() ++ lineage)
    }

  }


  def informativeValueTransitions = {
    valueTransitions(false, true)
      .filter(t => !GLOBAL_CONFIG.nonInformativeValues.contains(t.prev) && !GLOBAL_CONFIG.nonInformativeValues.contains(t.after))
  }

  def toCBRBDomain(id: String, STANDARD_TIME_FRAME_START: LocalDate, traintTimeEnd: LocalDate, isQuery: Boolean) = {
    val daysAsEpochDays = STANDARD_TIME_FRAME_START.toEpochDay to traintTimeEnd.toEpochDay by GLOBAL_CONFIG.granularityInDays
    val queryWildcard = ReservedChangeValues.NOT_EXISTANT_COL + "Q"
    val indexWildcard = ReservedChangeValues.NOT_EXISTANT_COL + "I"
    val dummyValue = "\u00A6\u00A9\u0F02\u0FCC"
    val values = daysAsEpochDays.flatMap(l => {
      val date = LocalDate.ofEpochDay(l)
      val v = this.valueAt(date)
      (isQuery,isWildcard(v)) match {
        case (false,false) => Seq(l + "." + Util.nullSafeToString(v),l+ "." + queryWildcard)
        case (false,true) => Seq(l + "." + indexWildcard)
        case (true,false) => Seq(l + "." + Util.nullSafeToString(v),l+ "." + indexWildcard)
        case (true,true) => Seq(l + "." + indexWildcard,l+ "." + queryWildcard)
      }
    })
    RoleDomain(id,values)
  }

  def exactMatchesWithoutWildcardPercentage(rl2Projected: RoleLineage, until: LocalDate) = {
    val results = exactlyMatchesWithoutWildcard(rl2Projected,GLOBAL_CONFIG.STANDARD_TIME_RANGE.filter(!_.isAfter(until)))
    results.filter(b => b).size / results.size.toDouble
  }

  def exactMatchWithoutWildcardCount(other:RoleLineage,until:LocalDate,ignoreWildcardInCount:Boolean = false) =
    exactlyMatchesWithoutWildcard(other,GLOBAL_CONFIG.STANDARD_TIME_RANGE.filter(!_.isAfter(until)),ignoreWildcardInCount).filter(b => b).size

  def toExactValueSequence(endTime:LocalDate) = {
    val timeRange = GLOBAL_CONFIG.STANDARD_TIME_RANGE.filter(!_.isAfter(endTime))
    val representativeWildcardValue = RoleLineage.WILDCARD_VALUES.head
    timeRange
      .map(ld => {
        val myVal = valueAt(ld)
        if(isWildcard(myVal)) representativeWildcardValue else myVal
      })
  }

  def exactlyMatchesWithoutWildcard(rl2Projected: RoleLineage, timeRange:IndexedSeq[LocalDate], ignoreWildcardInCount:Boolean = false) = {
    val meWithoutDecay = RoleLineage(lineage.filter(_._2!=ReservedChangeValues.DECAYED))
    val otherWithoutDecay = RoleLineage(rl2Projected.lineage.filter(_._2!=ReservedChangeValues.DECAYED))
    timeRange
      .map(ld => {
        val myVal = meWithoutDecay.valueAt(ld)
        val otherVal = otherWithoutDecay.valueAt(ld)
        myVal == otherVal && (!ignoreWildcardInCount || !isWildcard(myVal))
      })
  }

  def exactlyMatchesWithoutDecay(rl2Projected: RoleLineage,until:LocalDate) = {
    exactlyMatchesWithoutWildcard(rl2Projected,GLOBAL_CONFIG.STANDARD_TIME_RANGE.filter(!_.isAfter(until))).forall(b => b)
  }

  def exactlyMatchesWithoutDecayInTimePeriod(rl2Projected: RoleLineage,timePeriod:IndexedSeq[LocalDate]) = {
    exactlyMatchesWithoutWildcard(rl2Projected,timePeriod).forall(b => b)
  }


  def toRoleAsDomain(id:String, STANDARD_TIME_FRAME_START: LocalDate, traintTimeEnd: LocalDate) = {
    val daysAsEpochDays = STANDARD_TIME_FRAME_START.toEpochDay to traintTimeEnd.toEpochDay by GLOBAL_CONFIG.granularityInDays
    val values = daysAsEpochDays.map(l => {
      val date = LocalDate.ofEpochDay(l)
      val v = this.valueAt(date)
      if(isWildcard(v))
        ReservedChangeValues.NOT_EXISTANT_COL + "_" + l
      else
        Util.nullSafeToString(v) + "_" +  l
    })
    RoleDomain(id,values)
  }

  def removeDECAYED(DECAYED: String) = {
    val lineageNew = lineage.toIndexedSeq
      .filter(_._2!=DECAYED)
    val lineageFinal = lineageNew.zipWithIndex
      .filter{case ((ld,v),i) => i==0 || v != lineageNew(i-1)._2}
      .map(_._1)
    RoleLineage(collection.mutable.TreeMap[LocalDate,Any]() ++ lineageFinal)
  }


  def isOfInterest(trainTimeEnd: LocalDate) = {
    val valueSetTrain = lineage.range(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd).map{ case(_,v) => v}.toSet
      .filter(v => !isWildcard(v))
    val valueSetTest = lineage.rangeFrom(trainTimeEnd).map(_._2).toSet
      .filter(v => !isWildcard(v))
    valueSetTrain.size>1 && valueSetTest.size>0
  }

  def changeFeaturesAsCSVString(endTime:LocalDate, featureCount:Int):String = {
    val withIndex = lineage
      .toIndexedSeq
      .zipWithIndex
    val features = withIndex
      .map{case ((date,value),i) =>
        val curEndTime = if(i==lineage.size-1) endTime else withIndex(i+1)._1._1
        val duration = ChronoUnit.DAYS.between(date,curEndTime)
        val valueToSerialize = Util.toCSVSafe(Util.nullSafeToString(value))
        //old: val res = s"COL V$i VAL $valueToSerialize COL T$i VAL ${date.toString} COL D$i VAL $duration"
        val res = s"${date.toString},$valueToSerialize,$duration"
        res
      }
      .take(featureCount)
    if(features.size<featureCount)
      (features ++ List.fill(featureCount-features.size)(",,")).mkString(",")
    else
      features.mkString(",")
  }

  def dittoString(endTime: LocalDate,idString:Option[String]):String = {
    assert(!lineage.lastKey.isAfter(endTime))
    val withIndex = lineage
      .toIndexedSeq
      .zipWithIndex
    withIndex
      .map{case ((date,value),i) =>
        val curEndTime = if(i==lineage.size-1) endTime else withIndex(i+1)._1._1
        val duration = ChronoUnit.DAYS.between(date,curEndTime)
        val valueToSerialize = Util.toDittoSaveString(Util.nullSafeToString(value))
        //old: val res = s"COL V$i VAL $valueToSerialize COL T$i VAL ${date.toString} COL D$i VAL $duration"
        val res = s"${idString.getOrElse("")} COL ${date.toString} VAL $valueToSerialize COL D$i VAL $duration"
        res
      }
      .mkString(" ")
  }


  def valueSequenceBefore(trainTimeEnd: LocalDate, includeWildcard:Boolean=true) = {
    val iterator = lineage.iterator
    var curElem = iterator.nextOption()
    val valueSequence = collection.mutable.ArrayBuffer[Any]()
    while(curElem.isDefined && curElem.get._1.isBefore(trainTimeEnd)){
      val value = curElem.get._2
      if((valueSequence.isEmpty || valueSequence.last != value) && (includeWildcard || !isWildcard(value)))
        valueSequence += value
      curElem = iterator.nextOption()
    }
    valueSequence
  }

  def nonWildcardValueSetBefore(trainTimeEnd: LocalDate) = {
    val iterator = lineage.iterator
    var curElem = iterator.nextOption()
    val valueSet = collection.mutable.HashSet[Any]()
    while(curElem.isDefined && curElem.get._1.isBefore(trainTimeEnd)){
      if(!isWildcard(curElem.get._2))
        valueSet.add(curElem.get._2)
      curElem = iterator.nextOption()
    }
    valueSet
  }


  def isNumeric = {
    lineage.values.forall(v => RoleLineage.isWildcard(v) || GLOBAL_CONFIG.nonInformativeValues.contains(v) || v.toString.matches(digitRegex))
  }

  //returns duration in days
  def nonWildcardDuration(timeRangeEnd:LocalDate/*,begin:Option[LocalDate]=None*/) = {
    val withIndex = lineage
      .zipWithIndex
      .toIndexedSeq
      .filter(_._1._1.isBefore(timeRangeEnd))
    var period:Long = 0
    //if(begin.isDefined) period += Period.between(begin.get,withIndex.head._1)
    withIndex.map{case ((ld,v),i) => {
      val curBegin = ld
      val end = if(i!=withIndex.size-1 && withIndex(i+1)._1._1.isBefore(timeRangeEnd)) withIndex(i+1)._1._1 else timeRangeEnd
      if(!isWildcard(v))
        period = period + (ChronoUnit.DAYS.between(curBegin,end))
    }}
    period
  }

  def toShortString: String = {
    val withoutWIldcard = lineage
      .filter(v => !isWildcard(v._2))
      .toIndexedSeq
      .zipWithIndex
    val withoutDuplicates = withoutWIldcard
      .filter { case ((t, v), i) => i == 0 || v != withoutWIldcard(i - 1)._1._2 }
      .map(_._1)
      .zipWithIndex
    "<" + withoutDuplicates
      .map{case ((t,v),i) => {
        val begin = t
        val end = if(i==withoutDuplicates.size-1) "?" else withoutDuplicates(i+1)._1._1.toString
        begin.toString + "-" + end + ":" +v
      }}//(_._1._2)
      .mkString(",") + ">"
  }

  def toIdentifiedRoleLineage(id: String) = RoleLineageWithID(id,toSerializationHelper)


  def projectToTimeRange(timeRangeStart: LocalDate, timeRangeEnd: LocalDate) = {
    val prevStart = lineage.firstKey
    val afterStart = lineage.filter { case (k, v) => !k.isBefore(timeRangeStart) && !k.isAfter(timeRangeEnd) }
    if(afterStart.isEmpty){
      val last = lineage.maxBefore(timeRangeStart).get
      RoleLineage(mutable.TreeMap((timeRangeStart,last._2)))
    } else{
      if(afterStart.firstKey!=timeRangeStart){
        if(!lineage.maxBefore(afterStart.firstKey).isDefined){
          println("what?")
          println(this)
          println(this.lineage)
          println(timeRangeStart)
          println(timeRangeEnd)
        }
        val before = lineage.maxBefore(afterStart.firstKey).get
        assert(before._1.isBefore(timeRangeStart))
        afterStart.put(timeRangeStart,before._2)
      }
      assert(afterStart.firstKey==timeRangeStart)
      assert(prevStart == lineage.firstKey)
      RoleLineage(afterStart)
    }
  }

  def keepOnlyStandardTimeRange = RoleLineage(lineage.filter(!_._1.isAfter(GLOBAL_CONFIG.STANDARD_TIME_FRAME_END)))

  def toSerializationHelper = {
    RoleLineageWithHashMap(lineage.toMap)
  }

   def valueAt(ts: LocalDate) = {
    if(lineage.contains(ts))
      lineage(ts)
    else {
      val res = lineage.maxBefore(ts)
      if(res.isDefined) {
        res.get._2
      } else {
        ReservedChangeValues.NOT_EXISTANT_ROW
      }
    }
  }

   override def toString: String = "[" + lineage.values.mkString("|") + "]"

   def firstTimestamp: LocalDate = lineage.firstKey

   def lastTimestamp: LocalDate = lineage.lastKey

   def getValueLineage: mutable.TreeMap[LocalDate, Any] = lineage

  def isWildcard(value: Any) = RoleLineage.isWildcard(value)

   def valuesAreCompatible(a: Any, b: Any,variant:RemainsValidVariant = RemainsValidVariant.STRICT): Boolean = {
    if(variant==RemainsValidVariant.STRICT) {
      if(isWildcard(a) || isWildcard(b)) true else a == b
    } else {
      assert(variant==RemainsValidVariant.CONTAINMENT)
      if(isWildcard(a) || isWildcard(b) || a==b) true
      else {
        val tokensA = if(a==null) Set("null") else a.toString.split("\\s").toSet
        val tokensB = if(b==null) Set("null") else b.toString.split("\\s").toSet
        tokensA.union(tokensB).size==Seq(tokensA.size,tokensB.size).max
      }
    }
  }

   def getCompatibleValue(a: Any, b: Any): Any = {
    if(a==b) a else if(isWildcard(a)) b else a
  }

  def valuesInInterval(ti: TimeInterval): IterableOnce[(TimeInterval, Any)] = {
    var toReturn = toIntervalRepresentation
      .withFilter{case (curTi,v) => !curTi.endOrMax.isBefore(ti.begin) && !curTi.begin.isAfter(ti.endOrMax)}
      .map{case (curTi,v) =>
        val end = Seq(curTi.endOrMax,ti.endOrMax).min
        val begin = Seq(curTi.begin,ti.begin).max
        (TimeInterval(begin,Some(`end`)),v)
      }
    if(ti.begin.isBefore(firstTimestamp))
      toReturn += ((TimeInterval(ti.begin,Some(firstTimestamp)),ReservedChangeValues.NOT_EXISTANT_ROW))
    toReturn
  }

   def fromValueLineage[V <: RoleLineage](lineage: RoleLineage): V = lineage.asInstanceOf[V]

   def fromTimestampToValue[V <: RoleLineage](asTree: mutable.TreeMap[LocalDate, Any]): V = RoleLineage(asTree).asInstanceOf[V]

   def nonWildCardValues: Iterable[Any] = getValueLineage.values.filter(!isWildcard(_))

   def numValues: Int = lineage.size

   def allTimestamps: Iterable[LocalDate] = lineage.keySet

   def WILDCARDVALUES: Set[Any] = WILDCARD_VALUES



  def getValueTransitionSet(ignoreWildcards: Boolean, granularityInDays: Int,trainTimeEnd:Option[LocalDate]=None) = {
    val vl = (if (ignoreWildcards) getValueLineage.filter { case (k, v) => !isWildcard(v) }.toIndexedSeq else getValueLineage.toIndexedSeq)
      .filter(t => t._1.isBefore(trainTimeEnd.getOrElse(LocalDate.MAX)))
    val end = if(trainTimeEnd.isDefined) trainTimeEnd.get else GLOBAL_CONFIG.STANDARD_TIME_FRAME_END
    val transitions = (1 until vl.size)
      .flatMap(i => {
        val prev = vl(i - 1)
        val cur = vl(i)
        //handle prev to prev
        val res = scala.collection.mutable.ArrayBuffer[ValueTransition]()
        if (cur._1.toEpochDay - prev._1.toEpochDay > granularityInDays) {
          //add Prev to Prev
          res += ValueTransition(prev._2, prev._2)
        }
        res += ValueTransition(prev._2, cur._2)
        //handle last:
        if (i == vl.size - 1 && cur._1.isBefore(end)) {
          res += ValueTransition(cur._2, cur._2)
        }
        res
      })
    transitions.toSet
  }

  def valueTransitions(includeSameValueTransitions: Boolean = false, ignoreInterleavedWildcards: Boolean = true): Set[ValueTransition] = {
    val lineage = if (ignoreInterleavedWildcards) getValueLineage.filter { case (k, v) => !isWildcard(v) } else getValueLineage
    if (!includeSameValueTransitions) {
      val vl = lineage
        .values
        //.filter(!isWildcard(_))
        .toIndexedSeq
      (1 until vl.size).map(i => ValueTransition(vl(i - 1), vl(i))).toSet
    } else {
      assert(!ignoreInterleavedWildcards) //implementation is not tailored to that yet
      val vl = lineage.toIndexedSeq
      val transitions = (1 until vl.size)
        .flatMap(i => {
          val prev = vl(i - 1)
          val cur = vl(i)
          //handle prev to prev
          val res = scala.collection.mutable.ArrayBuffer[ValueTransition]()
          if (cur._1.toEpochDay - prev._1.toEpochDay > 1) {
            //add Prev to Prev
            res += ValueTransition(prev._2, prev._2)
          }
          res += ValueTransition(prev._2, cur._2)
          //handle last:
          if (i == vl.size - 1 && cur._1.isBefore(GLOBAL_CONFIG.STANDARD_TIME_FRAME_END)) {
            res += ValueTransition(cur._2, cur._2)
          }
          res
        })
      transitions.toSet
    }
  }


  private def notWCOrEmpty(prevValue1: Option[Any]): Boolean = {
    !prevValue1.isEmpty && !isWildcard(prevValue1.get)
  }

  private def getNonWCInterleavedOverlapEvidenceMultiSet(other: RoleLineage): Option[mutable.HashMap[ValueTransition, Int]] = {
    val res = this.tryMergeWithConsistent(other)
    if (!res.isDefined) {
      return None
    }
    val vl1 = this.getValueLineage
    val vl2 = other.getValueLineage
    val vl1Iterator = scala.collection.mutable.Queue() ++ vl1
    val vl2Iterator = scala.collection.mutable.Queue() ++ vl2
    val evidenceSet = mutable.HashMap[ValueTransition, Int]()
    var curValue1 = vl1Iterator.dequeue()._2
    var curValue2 = vl2Iterator.dequeue()._2
    var prevValue1: Option[Any] = None
    var prevValue2: Option[Any] = None
    while ((!vl1Iterator.isEmpty || !vl2Iterator.isEmpty)) {
      assert(isWildcard(curValue1) || isWildcard(curValue2) || curValue1 == curValue2)
      if (vl1Iterator.isEmpty) {
        prevValue2 = Some(curValue2)
        curValue2 = vl2Iterator.dequeue()._2
      } else if (vl2Iterator.isEmpty) {
        prevValue1 = Some(curValue1)
        curValue1 = vl1Iterator.dequeue()._2
      } else {
        val ts1 = vl1Iterator.head._1
        val ts2 = vl2Iterator.head._1
        if (ts1 == ts2) {
          prevValue1 = Some(curValue1)
          curValue1 = vl1Iterator.dequeue()._2
          prevValue2 = Some(curValue2)
          curValue2 = vl2Iterator.dequeue()._2
          if (!isWildcard(curValue1) && !isWildcard(curValue2) && notWCOrEmpty(prevValue1) && notWCOrEmpty(prevValue2)) {
            assert(prevValue1.get == prevValue2.get)
            assert(curValue1 == curValue2)
            val toAdd = ValueTransition(prevValue1.get, curValue1)
            val oldValue = evidenceSet.getOrElse(toAdd, 0)
            evidenceSet(toAdd) = oldValue + 1
          }
        } else if (ts1.isBefore(ts2)) {
          prevValue1 = Some(curValue1)
          curValue1 = vl1Iterator.dequeue()._2
        } else {
          assert(ts2.isBefore(ts1))
          prevValue2 = Some(curValue2)
          curValue2 = vl2Iterator.dequeue()._2
        }
      }

    }
    Some(evidenceSet)
  }

  private def getWCInterleavedOverlapEvidenceMultiSet(other: RoleLineage): Option[mutable.HashMap[ValueTransition, Int]] = {
    val res = this.tryMergeWithConsistent(other)
    if (!res.isDefined) {
      return None
    }
    val vl1 = this.getValueLineage
    val vl2 = other.getValueLineage
    val vl1Iterator = scala.collection.mutable.Queue() ++ vl1
    val vl2Iterator = scala.collection.mutable.Queue() ++ vl2
    val evidenceSet = mutable.HashMap[ValueTransition, Int]()
    var curValue1 = vl1Iterator.dequeue()._2
    var curValue2 = vl2Iterator.dequeue()._2
    var prevNonWCValue1: Option[Any] = None
    var prevNonWCValue2: Option[Any] = None
    while ((!vl1Iterator.isEmpty || !vl2Iterator.isEmpty)) {
      assert(isWildcard(curValue1) || isWildcard(curValue2) || curValue1 == curValue2)
      if (vl1Iterator.isEmpty) {
        if (!isWildcard(curValue2))
          prevNonWCValue2 = Some(curValue2)
        curValue2 = vl2Iterator.dequeue()._2
      } else if (vl2Iterator.isEmpty) {
        if (!isWildcard(curValue1))
          prevNonWCValue1 = Some(curValue1)
        curValue1 = vl1Iterator.dequeue()._2
      } else {
        val ts1 = vl1Iterator.head._1
        val ts2 = vl2Iterator.head._1
        if (ts1 == ts2) {
          if (!isWildcard(curValue1))
            prevNonWCValue1 = Some(curValue1)
          curValue1 = vl1Iterator.dequeue()._2
          if (!isWildcard(curValue2))
            prevNonWCValue2 = Some(curValue2)
          curValue2 = vl2Iterator.dequeue()._2
          if (!isWildcard(curValue1) && !isWildcard(curValue2) &&
            prevNonWCValue1.isDefined && prevNonWCValue2.isDefined &&
            prevNonWCValue1.get == prevNonWCValue2.get &&
            curValue1 != prevNonWCValue1.get
          ) {
            val toAdd = ValueTransition(prevNonWCValue1.get, curValue1)
            val oldValue = evidenceSet.getOrElse(toAdd, 0)
            evidenceSet(toAdd) = oldValue + 1
          }
        } else if (ts1.isBefore(ts2)) {
          if (!isWildcard(curValue1))
            prevNonWCValue1 = Some(curValue1)
          curValue1 = vl1Iterator.dequeue()._2
        } else {
          assert(ts2.isBefore(ts1))
          if (!isWildcard(curValue2))
            prevNonWCValue2 = Some(curValue2)
          curValue2 = vl2Iterator.dequeue()._2
        }
      }

    }
    Some(evidenceSet)
  }

  //=GLOBAL_CONFIG.ALLOW_INTERLEAVED_WILDCARDS_BETWEEN_EVIDENCE_TRANSITIONS
  def getOverlapEvidenceMultiSet(other: RoleLineage): collection.Map[ValueTransition, Int] = getOverlapEvidenceMultiSet(other, GLOBAL_CONFIG.ALLOW_INTERLEAVED_WILDCARDS_BETWEEN_EVIDENCE_TRANSITIONS)

  def getStrictCompatibilityTimePercentage(other:RoleLineage,timeEnd:LocalDate) = {
    val absCompatibleTime = getAbsStrictCompatibleTime(other,timeEnd)
    absCompatibleTime.toDouble / ChronoUnit.DAYS.between(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,timeEnd).toDouble
  }

  def getCompatibilityTimePercentage(other:RoleLineage,timeEnd:LocalDate) = {
    val absCompatibleTime = getAbsCompatibleTime(other,timeEnd)
    absCompatibleTime.toDouble / ChronoUnit.DAYS.between(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,timeEnd).toDouble
  }

  def nonWildcardValueAtOrBefore(ld: LocalDate) = {
    val toReturn = lineage
      .filter(d => !d._1.isAfter(ld))
      .toIndexedSeq
      .reverse
      .find{case (_,v) => !isWildcard(v)}
      .map(_._2)
    toReturn
  }

  def nonWildcardValueAtOrAfter(ld: LocalDate) = {
    val toReturn = lineage
      .iteratorFrom(ld)
      .find{case (_,v) => !isWildcard(v)}
      .map(_._2)
    toReturn
  }

  def getAbsStrictCompatibleTime(other: RoleLineage, until:LocalDate) = {
    val timeRange = GLOBAL_CONFIG.STANDARD_TIME_RANGE.filter(_.isBefore(until))
    timeRange
      .map(ld => {
        val myVal = valueAt(ld)
        val otherVal = other.valueAt(ld)
        (isWildcard(myVal),isWildcard(otherVal)) match {
          case (true,true) => true
          case (true,false) => nonWildcardValueAtOrBefore(ld).getOrElse(None) == otherVal || nonWildcardValueAtOrAfter(ld).getOrElse(None) == otherVal
          case (false,true) => other.nonWildcardValueAtOrBefore(ld).getOrElse(None) == myVal || other.nonWildcardValueAtOrAfter(ld).getOrElse(None) == myVal
          case (false,false) => myVal == otherVal
        }
      })
      .map(b => if(b) GLOBAL_CONFIG.granularityInDays else 0)
      .sum
  }

  def getAbsCompatibleTime(other:RoleLineage,timeEnd:LocalDate) = {
    val changePoints = new CommonPointOfInterestIterator(this, other)
      .toIndexedSeq
      .filter(cp => cp.pointInTime.isBefore(timeEnd))
    var compatibleTimes = changePoints
      .map(cp => {
        if(cp.prevWasCompatible){
          ChronoUnit.DAYS.between(cp.prevPointInTime,cp.pointInTime)
        } else {
          0
        }
      })
    if(changePoints.isEmpty)
      println() //TODO!
    if(changePoints.last.pointInTime.isBefore(timeEnd) && changePoints.last.isCompatible){
      compatibleTimes = compatibleTimes ++ Seq(ChronoUnit.DAYS.between(changePoints.last.pointInTime,timeEnd))
    }
    if(compatibleTimes.size==0)
      0
    else
      compatibleTimes.sum
  }

  def getOverlapEvidenceCount(other: RoleLineage): Int = getOverlapEvidenceCount(other, GLOBAL_CONFIG.ALLOW_INTERLEAVED_WILDCARDS_BETWEEN_EVIDENCE_TRANSITIONS)

  def getOverlapEvidenceMultiSet(other: RoleLineage, allowInterleavedWildcards: Boolean) = {
    if (!allowInterleavedWildcards) {
      val res = getNonWCInterleavedOverlapEvidenceMultiSet(other)
      res.get
    }
    else {
      val res = getWCInterleavedOverlapEvidenceMultiSet(other)
      res.get
    }
  }

  def getOverlapEvidenceCount(other: RoleLineage, allowInterleavedWildcards: Boolean) = {
    if (!allowInterleavedWildcards) {
      val multiSet = getNonWCInterleavedOverlapEvidenceMultiSet(other)
      if (multiSet.isDefined) multiSet.get.values.sum else -1
    } else {
      val multiSet = getWCInterleavedOverlapEvidenceMultiSet(other)
      if (multiSet.isDefined) multiSet.get.values.sum else -1
    }
  }

  def allNonWildcardTimestamps: Iterable[LocalDate] = {
    getValueLineage
      .filter(t => !isWildcard(t._2))
      .keySet
  }

  def isConsistentWith(v2: RoleLineage, minTimePercentage: Double) = {
    val commonPointOfInterestIterator = new CommonPointOfInterestIterator(this,v2)
    var validDuration = 0
    var invalidDuration = 0
    commonPointOfInterestIterator
      .foreach(cp => {
        var durationToAdd = (cp.pointInTime.toEpochDay - cp.prevPointInTime.toEpochDay).toInt
        if(valuesAreCompatible(cp.curValueA,cp.curValueB)){
          validDuration +=durationToAdd
        } else {
          invalidDuration +=durationToAdd
        }
      })
    validDuration / (validDuration+invalidDuration).toDouble > minTimePercentage
  }

  def countChanges(changeCounter: UpdateChangeCounter): (Int,Int) = {
    changeCounter.countFieldChanges(this)
  }

  def toIntervalRepresentation:mutable.TreeMap[TimeInterval,Any] = {
    val asLineage = getValueLineage.toIndexedSeq
    mutable.TreeMap[TimeInterval,Any]() ++ (0 until asLineage.size).map( i=> {
      val (ts,value) = asLineage(i)
      if(i==asLineage.size-1)
        (TimeInterval(ts,None),value)
      else
        (TimeInterval(ts,Some(asLineage(i+1)._1.minusDays(1))),value)
    })
  }

  def getOverlapInterval(a: (TimeInterval, Any), b: (TimeInterval, Any),variant:RemainsValidVariant = RemainsValidVariant.STRICT): (TimeInterval, Any) = {
    assert(a._1.begin==b._1.begin)
    if(!valuesAreCompatible(a._2,b._2,variant)) {
      println()
    }
    assert(valuesAreCompatible(a._2,b._2,variant))
    val earliestEnd = Seq(a._1.endOrMax,b._1.endOrMax).minBy(_.toEpochDay)
    val endTime = if(earliestEnd==LocalDate.MAX) None else Some(earliestEnd)
    (TimeInterval(a._1.begin,endTime),getCompatibleValue(a._2,b._2))
  }

  def tryMergeWithConsistent[V <: RoleLineage](other: V,variant:RemainsValidVariant = RemainsValidVariant.STRICT): Option[V] = {
    val myLineage = scala.collection.mutable.ArrayBuffer() ++ this.toIntervalRepresentation
    val otherLineage = scala.collection.mutable.ArrayBuffer() ++ other.toIntervalRepresentation.toBuffer
    if(myLineage.isEmpty){
      return if(otherLineage.isEmpty) Some(fromValueLineage[V](RoleLineage())) else None
    } else if(otherLineage.isEmpty){
      return if(myLineage.isEmpty) Some(fromValueLineage[V](RoleLineage())) else None
    }
    val newLineage = mutable.ArrayBuffer[(TimeInterval,Any)]()
    var myIndex = 0
    var otherIndex = 0
    var incompatible = false
    while((myIndex < myLineage.size || otherIndex < otherLineage.size ) && !incompatible) {
      assert(myIndex < myLineage.size && otherIndex < otherLineage.size)
      val (myInterval,myValue) = myLineage(myIndex)
      val (otherInterval,otherValue) = otherLineage(otherIndex)
      assert(myInterval.begin == otherInterval.begin)
      var toAppend:(TimeInterval,Any) = null
      if(myInterval==otherInterval){
        if(!valuesAreCompatible(myValue,otherValue,variant)){
          incompatible=true
        } else {
          toAppend = (myInterval, getCompatibleValue(myValue, otherValue))
          myIndex += 1
          otherIndex += 1
        }
      } else if(myInterval<otherInterval){
        if(!valuesAreCompatible(myLineage(myIndex)._2,otherLineage(otherIndex)._2,variant)){
          incompatible = true
        } else {
          toAppend = getOverlapInterval(myLineage(myIndex), otherLineage(otherIndex),variant)
          //replace old interval with newer interval with begin set to myInterval.end+1
          otherLineage(otherIndex) = (TimeInterval(myInterval.end.get.plusDays(1), otherInterval.`end`), otherValue)
          myIndex += 1
        }
      } else{
        assert(otherInterval<myInterval)
        if(!valuesAreCompatible(myLineage(myIndex)._2,otherLineage(otherIndex)._2,variant)){
          incompatible = true
        } else {
          toAppend = getOverlapInterval(myLineage(myIndex), otherLineage(otherIndex),variant)
          myLineage(myIndex) = (TimeInterval(otherInterval.end.get.plusDays(1), myInterval.`end`), myValue)
          otherIndex += 1
        }
      }
      if(!incompatible) {
        if (!newLineage.isEmpty && newLineage.last._2 == toAppend._2) {
          //we replace the old interval by a longer one
          newLineage(newLineage.size - 1) = (TimeInterval(newLineage.last._1.begin, toAppend._1.`end`), toAppend._2)
        } else {
          //we simply append the new interval:
          newLineage += toAppend
        }
      }
    }
    if(incompatible)
      None
    else {
      val asTree = mutable.TreeMap[LocalDate, Any]() ++ newLineage.map(t => (t._1.begin, t._2))
      Some(fromTimestampToValue[V](asTree))
    }
  }

  def mergeWithConsistent[V<:RoleLineage](other: V):V = {
    tryMergeWithConsistent(other).get
  }

  def append[V<:RoleLineage](y: V): V = {
    assert(lastTimestamp.isBefore(y.firstTimestamp))
    fromTimestampToValue(this.getValueLineage ++ y.getValueLineage)
  }



}
object RoleLineage{

  def WILDCARD_VALUES:Set[Any] = Set(ReservedChangeValues.NOT_EXISTANT_DATASET,
    ReservedChangeValues.NOT_EXISTANT_COL,
    ReservedChangeValues.NOT_EXISTANT_ROW,
    ReservedChangeValues.NOT_EXISTANT_CELL,
    ReservedChangeValues.NOT_KNOWN_DUE_TO_NO_VISIBLE_CHANGE,
    ReservedChangeValues.DECAYED)

  def fromSerializationHelper(valueLineageWithHashMap: RoleLineageWithHashMap) = RoleLineage(mutable.TreeMap[LocalDate,Any]() ++ valueLineageWithHashMap.lineage)

  def isWildcard(value: Any) = WILDCARD_VALUES.contains(value)


}