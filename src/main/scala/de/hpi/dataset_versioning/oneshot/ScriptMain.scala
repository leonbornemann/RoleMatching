package de.hpi.dataset_versioning.oneshot

import java.io.{File, FileReader, PrintWriter}
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.{TemporalColumn, TemporalTable}
import de.hpi.dataset_versioning.data.change.{ChangeCube, ChangeExporter}
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.data.matching.ColumnMatchingRefinement
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.TemporalSchema
import de.hpi.dataset_versioning.data.simplified.RelationalDataset
import de.hpi.dataset_versioning.db_synthesis.baseline.DetailedBCNFChangeCounting.subdomain
import de.hpi.dataset_versioning.db_synthesis.baseline.config.{GLOBAL_CONFIG, DatasetInsertIgnoreFieldChangeCounter, NormalFieldChangeCounter, AllDeterminantIgnoreChangeCounter}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.fd.FunctionalDependencySet
import de.hpi.dataset_versioning.db_synthesis.sketches.column.TemporalColumnSketch
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}
import org.apache.commons.csv.{CSVFormat, CSVParser}

import scala.collection.mutable
import scala.io.Source

object ScriptMain extends App with StrictLogging{

  //get bcnf tables:
//  IOService.socrataDir = args(0)
//    val subdomain = args(1)
//    val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
//    val subdomainIds = subDomainInfo(subdomain)
//      .map(_.id)
//      .toIndexedSeq
//  //next filter:
//
//  val fullyDecomposed = DecomposedTemporalTable.filterNotFullyDecomposedTables(subdomain,subdomainIds)
//
//  println(subdomainIds.size)
//  println(fullyDecomposed.size)
  //print bcnf tables:

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
  val subdomainIds = subDomainInfo(subdomain)
    .map(_.id)
    .toIndexedSeq
  val counterNormal = new NormalFieldChangeCounter()
  val counterWithOutInitialInsert = new DatasetInsertIgnoreFieldChangeCounter()
  val pkIgnoreCounter = new AllDeterminantIgnoreChangeCounter(counterWithOutInitialInsert)
  val counters = Seq(counterNormal,counterWithOutInitialInsert,pkIgnoreCounter)
  val idsWithDecomposedTables = DecomposedTemporalTable.filterNotFullyDecomposedTables(subdomain, subdomainIds)
  val resFileWriter = new PrintWriter("bcnfBetter.txt")
  private val idsToProcess = idsWithDecomposedTables
    .sorted
  for(id <- idsToProcess){
    val tt = TemporalTable.load(id)
    val dtts = DecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain, id)
      .sortBy(_.id.bcnfID)
    val allBCNFTables = dtts
      .map(dtt => tt.project(dtt).projection)
    val ttKeyCols = dtts.flatMap(_.primaryKey.map(_.attrId)).toSet
    val colsInOriginal = tt.getTemporalColumns()
    val insertTime = tt.insertTime
    val nrowView = tt.rows.size
    val nrowBCNF = allBCNFTables.map(_.rows.size).sum
    val nFieldView = tt.attributes.size*tt.rows.size
    val nFieldBCNF = allBCNFTables.map(dtt => dtt.attributes.size*dtt.rows.size).sum
    resFileWriter.println(tt.id)
    println(tt.id)
    resFileWriter.print(s"Original View: ")
    print(s"Original View: ")
    val kvPairs:Seq[(String,Long)] = Seq(("nrow",tt.rows.size.toLong)) ++ counters.map(c => (c.name,c.countChanges(tt,ttKeyCols)))
    printKvPairs(kvPairs)
    printKvPairsToSTDOUT(kvPairs)
    val bcnfResults = allBCNFTables.map(bcnf => {
      val schemaString = bcnf.dtt.get.getSchemaString
      val bcnfKVPairs = Seq(("nrow",bcnf.rows.size.toLong)) ++ counters.map(c => (c.name,c.countChanges(bcnf,ttKeyCols)))
      (schemaString,bcnfKVPairs)
    })
    val bcnfAggregatedKvPairs = bcnfResults.map(_._2).reduce((a,b) => {
      val res = a.zip(b).map{t => {
        val (s:String,l1) = t._1
        val (s2:String,l2) = t._2
        val newString:String = s
        val newSum:Long = (l1+l2).toLong
        val smallRes = (newString,newSum)
        smallRes
      }}
      res
    })
    resFileWriter.print("BCNF Aggregated: ")
    print("BCNF Aggregated: ")
    printKvPairs(bcnfAggregatedKvPairs)
    printKvPairsToSTDOUT(bcnfAggregatedKvPairs)
    bcnfResults.foreach{case (schemaString,bcnfKVPairs) => {
      resFileWriter.print(schemaString)
      printKvPairs(bcnfKVPairs)
      print(schemaString)
      printKvPairsToSTDOUT(bcnfKVPairs)
    }}
    resFileWriter.println()
    println()
//    if(bcnfAggregatedKvPairs.toMap.get("#c_noKey&NoInsert").get > kvPairs.toMap.get("#c_noKey&NoInsert").get) {
//      val colsToCheck = tt.attributes.filter(a => !ttKeyCols.contains(a.attrId)).map(_.attrId)
//      val ttInsertTime = tt.insertTime
//      val allCOls = tt.getTemporalColumns().map(tc => pkIgnoreCounter.countColumnChanges(tc,ttInsertTime,ttKeyCols.contains(tc.attrID))).sum
//      val tableCount = pkIgnoreCounter.countChanges(tt,ttKeyCols)
//      println()
//      colsToCheck.foreach(c => {
//        val tc = tt.getTemporalColumns().filter(_.attrID==c).head
//        val countTT = pkIgnoreCounter.countColumnChanges(tc,ttInsertTime,false)
//        //BCNF tables:
//        val bcnfTables = allBCNFTables.filter(_.attributes.map(_.attrId).contains(c))
//        if(bcnfTables.size!=1){
//          println()
//        }
//        assert(bcnfTables.size==1)
//      })
//      println(tt.id)
//      assert(false)
//    }
//      val string = s"$id,${tt.attributes.size},$changesTTNoInsert,$changesBCNF,$nrowView,$nrowBCNF,$nFieldView,$nFieldBCNF"
//      println(string)
//      res.println(string)
  }
  resFileWriter.close()

  //stats about wildcard overlap:
//  IOService.socrataDir = args(0)
//  val subdomain = args(1)
//  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
//  val subdomainIds = subDomainInfo(subdomain)
//        .map(_.id)
//        .toIndexedSeq
//  val schemata = subdomainIds.map(id => TemporalSchema.load(id))
//  val allAttributes = schemata.flatMap(s => s.attributes.map(al => (s.id,al)))
//  var nonCoveredAttributeIDs = mutable.HashSet() ++ allAttributes.map(t => (t._1,t._2.attrId)).toSet
//  val chosenTimestamps = mutable.ArrayBuffer[LocalDate]()
//  val timerange = IOService.STANDARD_TIME_RANGE
//  while(chosenTimestamps.size<=100 && !nonCoveredAttributeIDs.isEmpty){
//    val chosen = getNextMostDiscriminatingTimestamp
//    chosenTimestamps += chosen
//  }
//  println(chosenTimestamps)
//  println(chosenTimestamps.sortBy(_.toEpochDay))
//
//  private def getNextMostDiscriminatingTimestamp = {
//    val byTimestamp = timerange
//      .withFilter(!chosenTimestamps.contains(_))
//      .map(t => {
//        val (isNonWildCard, isWildCard) = allAttributes
//          .filter(al => nonCoveredAttributeIDs.contains((al._1,al._2.attrId)))
//          .partition(al => al._2.valueAt(t)._2.exists)
//        (t, isNonWildCard, isWildCard)
//      })
//    val bestNextTs = byTimestamp.sortBy(-_._2.size)
//      .head
//    val nowCovered = bestNextTs._2.map(al => (al._1, al._2.attrId)).toSet
//    nonCoveredAttributeIDs = nonCoveredAttributeIDs.diff(nowCovered)
//    println(s"${bestNextTs._1} covers ${bestNextTs._2.size} new attributes leaving ${bestNextTs._3.size} still open")
//    bestNextTs._1
//  }


  //stats about schema overlap
//  IOService.socrataDir = args(0)
//  val subdomain = args(1)
//  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
//  val subdomainIds = subDomainInfo(subdomain)
//        .map(_.id)
//        .toIndexedSeq
//  val schemata = subdomainIds.map(id => TemporalSchema.load(id))
//  val nOverlaps = (0 until schemata.size).map( i=> {
//    val nOverlappingDatasets = (0 until schemata.size).map(j => {
//      val schema1 = schemata(i)
//      val schema2 = schemata(j)
//      val setA = schema1.attributes.flatMap(_.nameSet).toSet
//      val setB = schema2.attributes.flatMap(_.nameSet).toSet
//      val overlapCount = setA.intersect(setB).size
//      if(overlapCount >= Seq(schema1.attributes.size,schema2.attributes.size).min) 1 else 0
////      val anyNameOverlaps = !setA.intersect(setB).isEmpty
////      if (anyNameOverlaps) 1 else 0
//    }).sum - 1
//    nOverlappingDatasets
//  })
//  nOverlaps.foreach(println(_))
//  nOverlaps.groupBy(identity)
//    .map(t=> (t._1,t._2.size))
//    .toIndexedSeq
//    .sortBy(_._1)
//    .foreach(t => println(t))//println(s"${t._1},${t._2}")

  //read all sketches:
//  IOService.socrataDir = args(0)
//  val subdomain = args(1)
//  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
//  val subdomainIds = subDomainInfo(subdomain)
//      .map(_.id)
//      .toIndexedSeq
//  val a = subdomainIds.flatMap(id => {
//    TemporalColumnSketch.loadAll(id,Variant2Sketch.getVariantName)
//  })
//  println(a.size)

  private def printKvPairs(kvPairs: Seq[(String, Any)]) = {
    resFileWriter.println(toPrintString(kvPairs))
  }

  private def toPrintString(kvPairs: Seq[(String, Any)]) = {
    "   {" + kvPairs.map(t => t._1 + ":" + t._2).mkString(", ") + "}"
  }

  private def printKvPairsToSTDOUT(kvPairs: Seq[(String, Any)]) = {
    println(toPrintString(kvPairs))
  }

  println()
  //hash sketch size calculations:
//  var timestampSize = 1
//  var hashSize = 1
//  val NUM_TIMESTAMPS = 182
//
//  def variant1(timestampSize: Int, hashSize: Int) = hashSize*NUM_TIMESTAMPS
//  def variant2(timestampSize: Int, hashSize: Int,nChanges:Int) = timestampSize*hashSize*nChanges
//  def totalSizeMB(sketchSizeByte:Int) = sketchSizeByte*(6224770/1000000)
//
//  val wcvariant1 = variant1(timestampSize,hashSize)
//  val bcvariant1 = variant1(timestampSize,hashSize)
//  val wcvariant2 = variant2(timestampSize,hashSize,NUM_TIMESTAMPS)
//  println("variant,Hash Size [Byte],Worst Case FL sketch Size [Byte],Best Case FL sketch Size[Byte],Total Size [MB]")
//  println(Seq(1,1,variant1(1,1),variant1(1,1),totalSizeMB(wcvariant2)).mkString(" "))
//  println(Seq(1,4,variant1(1,4),variant1(1,4),totalSizeMB(variant1(1,4))).mkString(" "))


  //temporal column export test:
//  IOService.socrataDir = args(0)
//  val subdomain = args(1)
//  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
//  val subdomainIds = subDomainInfo(subdomain)
//    .map(_.id)
//    .toIndexedSeq
//  val id = subdomainIds.head
//  val tt = TemporalTable.load(id)
//  val cols = tt.getTemporalColumns()
//  cols.foreach(col => col.writeToStandardFile())
//  val colsLoaded = tt.attributes.map(al => al.attrId)
//    .map(TemporalColumn.load(id,_))
//  println()

  //realistic sketch size:
//  IOService.socrataDir = args(0)
//  val subdomain = args(1)
//  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
//  val subdomainIds = subDomainInfo(subdomain)
//    .map(_.id)
//    .toIndexedSeq
//  var totalFieldLineages:Long = 0
//  var nAttributes = 0
//  subdomainIds.foreach(id => {
//    val tt = TemporalTable.load(id)
//    logger.debug(s"Found table $id with ${tt.attributes.size} #columns and ${tt.attributes.size * tt.rows.size} total field lineages")
//    nAttributes += tt.attributes.size
//    totalFieldLineages += tt.attributes.size * tt.rows.size
//    logger.debug(s"current avg field lineage count per column: ${totalFieldLineages / nAttributes.toDouble}")
//  })
//  logger.debug(s"final num field Lineages: ${totalFieldLineages}")
//  logger.debug(s"final num attrs: $nAttributes")
//  logger.debug(s"average per attribute: ${totalFieldLineages / nAttributes.toDouble}")

//  val allTimestamps = (IOService.STANDARD_TIME_FRAME_START.toEpochDay to IOService.STANDARD_TIME_FRAME_END.toEpochDay)
//    .toIndexedSeq
//    .map(LocalDate.ofEpochDay(_))
//
//  IOService.socrataDir = args(0)
//  val subdomain = args(1)
//  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
//  val subdomainIds = subDomainInfo(subdomain)
//      .map(_.id)
//      .toIndexedSeq
//  val temporalSchemata = subdomainIds.map(id => TemporalSchema.load(id))
//  val idsWithAtLeastOneSchemaChange = temporalSchemata.filter(temporalSchema => {
//      val schemaSizes = allTimestamps.map(ts => temporalSchema.valueAt(ts).filter(_._2.exists).size)
//      schemaSizes.filter(_!=0).toSet.size>1
//    })
//  println(idsWithAtLeastOneSchemaChange.size)
//  val dsHistory = DatasetVersionHistory.load()
//    .map(h => (h.id,h)).toMap
//  println(subdomainIds.filter(id => dsHistory(id).versionsWithChanges.size>1).size)
//  IOService.socrataDir = args(0)
//  val subdomain = args(1)
//    val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
//    val subdomainIds = subDomainInfo(subdomain)
//      .map(_.id)
//      .toIndexedSeq
//
//  val allTimestamps = (IOService.STANDARD_TIME_FRAME_START.toEpochDay to IOService.STANDARD_TIME_FRAME_END.toEpochDay)
//    .toIndexedSeq
//    .map(LocalDate.ofEpochDay(_))
//
//  def sharesAtLeast1Timestamp(attrA: AttributeLineage, attrB: AttributeLineage) = {
//      allTimestamps.exists(ts => attrA.valueAt(ts)._2.exists && attrB.valueAt(ts)._2.exists)
//  }
//
//  var numPossibleNewAttrMatches = 0
//  var numPossibleConfusions = 0
//  var dsWithPotentialMatch = mutable.HashMap[String,mutable.HashSet[Set[AttributeLineage]]]()
//  subdomainIds.foreach(id => {
//    val schema = TemporalSchema.load(id)
//    val attrs = schema.attributes
//    for(i <- 0 until attrs.size){
//      val attrA = attrs(i)
//      for(j <- (i+1) until attrs.size){
//        val attrB = attrs(j)
//        if(attrB.nameSet.intersect(attrA.nameSet).size!=0){
//          dsWithPotentialMatch.getOrElseUpdate(id,mutable.HashSet())
//            .add(Set(attrA,attrB))
//          if(sharesAtLeast1Timestamp(attrA,attrB)){
//            numPossibleConfusions +=1
//          } else{
//            numPossibleNewAttrMatches +=1
//          }
//        }
//      }
//    }
//  })
//  println(numPossibleConfusions)
//  println(numPossibleNewAttrMatches)
//
//  var totalMergesFound = 0
//  subdomainIds.foreach(id => {
//    val schema = TemporalSchema.load(id)
//    val attrs = schema.attributes
//    val extender = new ColumnMatchingRefinement(id)
//    val (mergesFound,result) = extender.getPossibleSaveMerges(attrs)
//    if(dsWithPotentialMatch.contains(id) && mergesFound == 0 ){
//      val tt = TemporalTable.load(id)
//      println()
//    }
//    val (mergesFound2,result2) = extender.getPossibleSaveMerges(attrs)
//    totalMergesFound += mergesFound
//  })
//  println(totalMergesFound)


  //LHS-size (key size) chosen by decomposition
//  IOService.socrataDir = args(0)
//  val subdomain = args(1)
//  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
//  val subdomainIds = subDomainInfo(subdomain)
//    .map(_.id)
//    .toIndexedSeq
//  val temporallyDecomposedTables = subdomainIds
//    .filter(id => DBSynthesis_IOService.getDecomposedTemporalTableDir(subdomain,id).exists())
//    .flatMap(id => {
//      val dtts = DecomposedTemporalTable.loadAll(subdomain, id)
//      dtts
//    })
//  temporallyDecomposedTables.map(_.originalFDLHS.size).groupBy(identity)
//    .toIndexedSeq
//    .sortBy(_._1)
//    .foreach(t => println(s"${t._1},${t._2.size}"))

  //FD discovery stats:
//  val lines = Source.fromFile("/home/leon/Desktop/statsNew.csv")
//    .getLines()
//    .toSeq
//    .tail
//    .map(_.split(",").map(_.toInt).toIndexedSeq)
//    .filter(_(0)!=0)
//  println(lines)
//  val fdIntersectionSelectivity = lines.map(l => l(1) / l(0).toDouble)
//  var relativeOverlap = lines.filter(l => l(0) != l(1)).map(l => l(4) / l(2).toDouble)
//  relativeOverlap = relativeOverlap ++ (0 until 42).map(_ => 1.0)
//  val avgOverlap = relativeOverlap.sum / relativeOverlap.size.toDouble
//  println(avgOverlap)
//  var fdIntersectionSelectivityWithoutEquality = lines.filter(l => l(0) != l(1)).map(l => l(1) / l(0).toDouble)
//  //account for thos with changes that are equal
//  fdIntersectionSelectivityWithoutEquality= fdIntersectionSelectivityWithoutEquality ++ (0 until 42).map(_ => 1.0)
//
//  println(fdIntersectionSelectivity.sum / fdIntersectionSelectivity.size.toDouble)
//  println(fdIntersectionSelectivity.size)
//  println(fdIntersectionSelectivityWithoutEquality.sum / fdIntersectionSelectivityWithoutEquality.size.toDouble)
//  println(fdIntersectionSelectivityWithoutEquality.size)
//
//  println(
//  )
//  fdIntersectionSelectivityWithoutEquality.foreach(println(_))
}
