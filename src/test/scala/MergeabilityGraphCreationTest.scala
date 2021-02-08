import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.index.BipartiteTupleIndex
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.BipartiteFieldLineageMatchGraph
import de.hpi.dataset_versioning.entropy.FieldLineageAsCharacterString

object MergeabilityGraphCreationTest extends App {

  val matchingsToRank = IndexedSeq(
    ("___BBBCCCD","BBBBBBCCCD"),
    ("___BBBCCCD","___BBBCCCD"),
    ("___BBBCCCD","CCCBBBCCCD"),
    ("___BBBCCCD","CBCBBBCCCD"),
    ("___BBBCCCD","AEABBBCCCD"),
    ("___BBBCCCD","AAABBBC___"),
    ("___BBBCCCD","___BBBC___"),
    ("___BBBCCCD","_____BC___"),
    ("___BBBCCCD","AAA_____CD"),
    ("___BBBCCCD","BBB_____CD"),
    ("___BBBCCCD","CCC_____CD"),
    ("___BBBCCCD","________CD")
  )

  def toFieldLineageAsCharacterString(_1: String) = {
    FieldLineageAsCharacterString(_1,"label")
  }

  val idLeft = DecomposedTemporalTableIdentifier("subdomain","left",0,Some(0))
  val idRight = DecomposedTemporalTableIdentifier("subdomain","right",0,Some(0))
  val left = FieldLineageAsCharacterString.toAssociationTable(IndexedSeq(toFieldLineageAsCharacterString(matchingsToRank.head._1)),idLeft)
  val right = FieldLineageAsCharacterString.toAssociationTable(matchingsToRank.map(t => toFieldLineageAsCharacterString(t._2)),idRight)
  val mg = new BipartiteFieldLineageMatchGraph(left.tupleReferences,right.tupleReferences)
  assert(mg.edges.size==12)
  assert(mg.edges.map(_.tupleReferenceB.rowIndex).toIndexedSeq.sorted == (0 until matchingsToRank.size))
}
