package de.hpi.dataset_versioning.db_synthesis.baseline

import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.{SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.DataBasedMatchCalculator
import de.hpi.dataset_versioning.io.IOService

import java.time.LocalDate
import scala.io.Source

object TopDownMain extends App {
  val hist = Source.fromFile("/home/leon/data/dataset_versioning/socrata/value_histogram.csv")
    .getLines()
    .toIndexedSeq
    .map(l => {
      val vals = l.split(",").toIndexedSeq
      (LocalDate.parse(vals(0)),vals(1),vals(2).toInt)
    })
  val byValue = hist
    .groupBy(_._2)
    .map{case (k,v) => (k,v.map(_._3).max)}
  byValue
    .toIndexedSeq
    .sortBy(l => -l._2)
    .take(100)
    .foreach(t => println(t._1.substring(0,Math.min(t._1.length,10)) + ":\t\t\t" + t._2))
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  GLOBAL_CONFIG.INDEX_DEPTH = args(2).toInt
  val countChangesForALlSteps = if(args.size>=4) args(3).toBoolean else true
  val toIgnore = if(args.size>=5) args(4).split(",").toSet else Set[String]()

  val dttIDA = DecomposedTemporalTableIdentifier(subdomain, viewID = "m6dm-c72p", bcnfID =2, associationID = Some(0))
  val dttIDB = DecomposedTemporalTableIdentifier(subdomain, viewID = "rsxa-ify5", bcnfID =1, associationID = Some(3))
  val tableA = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(dttIDA)
  val tableB = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(dttIDB)
  val tableASketch = SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch.loadFromStandardOptimizationInputFile(dttIDA)
  val tableBSketch = SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch.loadFromStandardOptimizationInputFile(dttIDB)

  val matchCalculator = new DataBasedMatchCalculator
//  val match_ = matchCalculator.calculateMatch(tableA,tableB)
//  println(match_)
  val ts = LocalDate.parse("2019-11-13")
  val rowsIdsA = Vector(0, 1, 2, 4, 5, 7, 8, 9, 10, 11, 13, 14, 15, 16, 19, 23, 24, 25, 27, 28, 29, 30, 31, 33, 34, 35, 36, 39, 41, 42, 44, 46, 49, 51, 52, 55, 56, 59, 60, 61, 66, 67, 68, 70, 71, 72, 73, 75, 77, 78, 79, 80, 82, 83, 84, 85, 87, 89, 93, 97, 99, 101, 102, 103, 106, 107, 110, 111, 112, 113, 114, 116, 117, 118, 119, 120, 121, 122, 123, 124, 126, 127, 128, 130, 131, 132, 133, 134, 135, 136, 139, 140, 141, 142, 143, 146, 147, 148, 149, 150, 151, 154, 155, 156, 157, 158, 159, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 179, 180, 181, 182, 183, 184, 185, 186, 187, 189, 190, 191, 193, 194, 195, 198, 199, 200, 201, 202, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 221, 222, 223, 224, 225, 227, 230, 231, 233, 234, 235, 236, 237, 239, 240, 242, 243, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 261, 263, 264, 265, 266, 267, 268, 270, 271, 272, 274, 275, 276, 277, 278, 279, 280, 281, 282, 284, 285, 286, 287, 288, 289, 290, 292, 293, 294, 295, 296, 297, 300, 302, 303, 304, 305, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 316, 318, 320, 321, 322, 323, 324, 325, 327, 329, 330, 332, 333, 335, 337, 338, 339, 340, 341, 342, 344, 345, 346, 347, 348, 349, 351, 353, 354, 355, 356, 357, 358, 359, 360, 361, 362, 363, 364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 376, 379, 380, 381, 382, 383, 384, 385, 386, 387, 389, 390, 391, 392, 393, 395, 396, 397, 398, 399, 400, 401, 402, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 419, 420, 421, 422, 423, 425, 427, 428, 429, 430, 431, 432, 434, 435, 436, 437, 439, 440, 441, 442, 444, 445, 446, 447, 449, 450, 451, 452, 454, 455, 456)
  val rowsIdsB = Vector(0, 1, 2, 3, 5, 7, 9, 11, 15, 36, 37, 39, 40, 41, 42, 43, 47, 53, 55, 61, 65, 66, 69, 73, 87, 89, 90, 106, 111, 112, 113, 114, 116, 117, 121, 124, 125, 126, 127, 130, 131, 134, 136, 139, 142, 143, 144, 145, 146, 147, 150, 151, 154, 155, 156, 157, 158, 160, 161, 165, 166, 167, 168, 169, 172, 186, 190, 192, 197, 203, 204, 207, 208, 214, 215, 217, 218, 219, 227, 229, 230, 231, 235, 243, 245, 248, 249, 251, 252, 253, 254, 255, 259, 262, 265, 266, 269, 272, 273, 277, 278, 283, 285, 286, 291, 293, 294, 296, 299, 300, 301, 302, 303, 304, 306, 307, 308, 309, 312, 313, 314, 316, 317, 319, 320, 321, 322, 324, 325, 326, 327, 328, 331, 332, 333, 334, 337, 340, 343, 352, 355, 356, 357, 358, 359, 361, 362, 363, 364, 366, 367, 369, 370, 383, 386, 391, 393, 405, 406, 409, 410, 412, 413, 418, 421, 427, 429, 431, 433, 435, 441, 445, 446, 447, 448, 449, 450, 452, 454, 457, 458, 460, 461, 462, 463, 468, 469, 470, 476, 477, 482, 488, 499, 500, 501, 502, 503, 504, 505, 507, 508, 509, 510, 513, 527, 538, 541, 553, 558, 559, 561, 564, 566, 569, 571, 575, 579, 584, 587, 590, 591, 592, 593, 594, 595, 596, 597, 598, 599, 600, 601, 602, 603, 604, 606, 607, 608, 609, 610, 611, 612, 613, 614, 618, 619, 620, 635, 636, 637, 638, 639, 640, 641, 643, 644, 646, 647, 648, 649, 650, 651, 652, 653, 664, 665, 667, 670, 678, 681, 685, 687, 688, 693, 700, 713, 717, 720, 721, 724, 736, 740, 741, 744, 745, 746, 747, 748, 749, 750, 751, 753, 754, 758, 759, 760, 765, 769, 771, 777, 779, 780, 782, 798, 799, 803, 804, 805, 806, 807, 808, 809, 810, 811, 812, 814, 817, 821, 822, 824, 829, 831, 834, 836, 839, 840, 843, 844, 847, 849, 853, 855, 868, 870, 871, 872, 873, 875, 876, 877, 878, 879, 880, 881)
  val rowsA = rowsIdsA.map(i => tableA.getDataTuple(i).head.valueAt(ts))
  val rowsB = rowsIdsB.map(i => tableB.getDataTuple(i).head.valueAt(ts))
  val rowsASketches = rowsIdsA.map(i => tableASketch.getDataTuple(i).head.valueAt(ts))
  val rowsBSketches = rowsIdsB.map(i => tableBSketch.getDataTuple(i).head.valueAt(ts))

  val topDown = new TopDown(subdomain,toIgnore)
  topDown.synthesizeDatabase(countChangesForALlSteps)


}
