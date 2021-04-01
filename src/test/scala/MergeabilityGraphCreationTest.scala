import de.hpi.tfm.compatibility.graph.fact.bipartite.BipartiteFactMatchCreator
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.score_exploration.FieldLineageAsCharacterString

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

  val idLeft = AssociationIdentifier("subdomain","left",0,Some(0))
  val idRight = AssociationIdentifier("subdomain","right",0,Some(0))
  val left = FieldLineageAsCharacterString.toAssociationTable(IndexedSeq(toFieldLineageAsCharacterString(matchingsToRank.head._1)),idLeft)
  val right = FieldLineageAsCharacterString.toAssociationTable(matchingsToRank.map(t => toFieldLineageAsCharacterString(t._2)),idRight)
  val mg = new BipartiteFactMatchCreator(left.tupleReferences,right.tupleReferences,null)
  assert(mg.facts.size==12)
  assert(mg.facts.map(_.tupleReferenceB.rowIndex).toIndexedSeq.sorted == (0 until matchingsToRank.size))
}
